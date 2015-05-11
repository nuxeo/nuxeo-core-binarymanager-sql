/*
 * (C) Copyright 2010-2014 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.blob.binary.Binary;
import org.nuxeo.ecm.core.blob.binary.BinaryGarbageCollector;
import org.nuxeo.ecm.core.blob.binary.BinaryManager;
import org.nuxeo.ecm.core.blob.binary.BinaryManagerStatus;
import org.nuxeo.ecm.core.blob.binary.CachingBinaryManager;
import org.nuxeo.ecm.core.blob.binary.FileStorage;
import org.nuxeo.ecm.core.storage.StorageException;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Column;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Database;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Table;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.Dialect;
import org.nuxeo.runtime.datasource.DataSourceHelper;

/**
 * A Binary Manager that stores binaries as SQL BLOBs.
 * <p>
 * The BLOBs are cached locally on first access for efficiency.
 * <p>
 * Because the BLOB length can be accessed independently of the binary stream, it is also cached in a simple text file
 * if accessed before the stream.
 */
public class SQLBinaryManager extends CachingBinaryManager {

    private static final Log log = LogFactory.getLog(SQLBinaryManager.class);

    public static final String DS_PROP = "datasource";

    public static final String DS_PREFIX = "datasource=";

    public static final String TABLE_PROP = "table";

    public static final String TABLE_PREFIX = "table=";

    public static final String CACHE_SIZE_PROP = "cacheSize";

    public static final String CACHE_SIZE_PREFIX = "cachesize=";

    public static final String DEFAULT_CACHE_SIZE = "10M";

    public static final String COL_ID = "id";

    public static final String COL_BIN = "bin";

    public static final String COL_MARK = "mark"; // for mark & sweep GC

    protected String dataSourceName;

    protected DataSource dataSource;

    protected String checkSql;

    protected String putSql;

    protected String getSql;

    protected String getLengthSql;

    protected String gcStartSql;

    protected String gcMarkSql;

    protected String gcStatsSql;

    protected String gcSweepSql;

    protected static boolean disableCheckExisting; // for unit tests

    protected static boolean resetCache; // for unit tests

    @Override
    public void initialize(String blobProviderId, Map<String, String> properties) throws IOException {
        super.initialize(blobProviderId, properties);

        dataSourceName = null;
        String tableName = null;
        String cacheSizeStr = null;
        String key = properties.get(BinaryManager.PROP_KEY);
        key = StringUtils.defaultIfBlank(key, "");
        for (String part : key.split(",")) {
            if (part.startsWith(DS_PREFIX)) {
                dataSourceName = part.substring(DS_PREFIX.length()).trim();
            }
            if (part.startsWith(TABLE_PREFIX)) {
                tableName = part.substring(TABLE_PREFIX.length()).trim();
            }
            if (part.startsWith(CACHE_SIZE_PREFIX)) {
                cacheSizeStr = part.substring(CACHE_SIZE_PREFIX.length()).trim();
            }
        }
        if (StringUtils.isBlank(dataSourceName)) {
            dataSourceName = properties.get(DS_PROP);
            if (StringUtils.isBlank(dataSourceName)) {
                throw new RuntimeException("Missing " + DS_PROP + " in binaryManager configuration");
            }
        }
        if (StringUtils.isBlank(tableName)) {
            tableName = properties.get(TABLE_PROP);
            if (StringUtils.isBlank(tableName)) {
                throw new RuntimeException("Missing " + TABLE_PROP + " in binaryManager configuration");
            }
        }
        if (StringUtils.isBlank(cacheSizeStr)) {
            cacheSizeStr = properties.get(CACHE_SIZE_PROP);
            if (StringUtils.isBlank(cacheSizeStr)) {
                cacheSizeStr = DEFAULT_CACHE_SIZE;
            }
        }

        try {
            dataSource = DataSourceHelper.getDataSource(dataSourceName);
        } catch (NamingException e) {
            throw new IOException("Cannot find datasource: " + dataSourceName, e);
        }

        // create the SQL statements used
        createSql(tableName);

        // create file cache
        initializeCache(cacheSizeStr, new SQLFileStorage());
        createGarbageCollector();
    }

    protected void createGarbageCollector() {
        garbageCollector = new SQLBinaryGarbageCollector(this);
    }

    protected void createSql(String tableName) throws IOException {
        Dialect dialect = getDialect();
        Database database = new Database(dialect);
        Table table = database.addTable(tableName);
        ColumnType dummytype = ColumnType.STRING;
        Column idCol = table.addColumn(COL_ID, dummytype, COL_ID, null);
        Column binCol = table.addColumn(COL_BIN, dummytype, COL_BIN, null);
        Column markCol = table.addColumn(COL_MARK, dummytype, COL_MARK, null);

        checkSql = String.format("SELECT 1 FROM %s WHERE %s = ?", table.getQuotedName(), idCol.getQuotedName());
        putSql = String.format("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)", table.getQuotedName(),
                idCol.getQuotedName(), binCol.getQuotedName(), markCol.getQuotedName());
        getSql = String.format("SELECT %s FROM %s WHERE %s = ?", binCol.getQuotedName(), table.getQuotedName(),
                idCol.getQuotedName());
        getLengthSql = String.format("SELECT %s(%s) FROM %s WHERE %s = ?", dialect.getBlobLengthFunction(),
                binCol.getQuotedName(), table.getQuotedName(), idCol.getQuotedName());

        gcStartSql = String.format("UPDATE %s SET %s = ?", table.getQuotedName(), markCol.getQuotedName());
        gcMarkSql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", table.getQuotedName(), markCol.getQuotedName(),
                idCol.getQuotedName());
        gcStatsSql = String.format("SELECT COUNT(*), SUM(%s(%s)) FROM %s WHERE %s = ?", dialect.getBlobLengthFunction(),
                binCol.getQuotedName(), table.getQuotedName(), markCol.getQuotedName());
        gcSweepSql = String.format("DELETE FROM %s WHERE %s = ?", table.getQuotedName(), markCol.getQuotedName());
    }

    protected Dialect getDialect() throws IOException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            return Dialect.createDialect(connection, null);
        } catch (StorageException e) {
            throw new IOException(e);
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error(e, e);
                }
            }
        }
    }

    protected static void logSQL(String sql, Serializable... values) {
        if (!log.isTraceEnabled()) {
            return;
        }
        StringBuilder buf = new StringBuilder();
        int start = 0;
        for (Serializable v : values) {
            int index = sql.indexOf('?', start);
            if (index == -1) {
                // mismatch between number of ? and number of values
                break;
            }
            buf.append(sql, start, index);
            buf.append(loggedValue(v));
            start = index + 1;
        }
        buf.append(sql, start, sql.length());
        log.trace("(bin) SQL: " + buf.toString());
    }

    protected static String loggedValue(Serializable value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            String v = (String) value;
            return "'" + v.replace("'", "''") + "'";
        }
        return value.toString();
    }

    protected static boolean isDuplicateKeyException(SQLException e) {
        String sqlState = e.getSQLState();
        if ("23000".equals(sqlState)) {
            // MySQL: Duplicate entry ... for key ...
            // Oracle: unique constraint ... violated
            // SQL Server: Violation of PRIMARY KEY constraint
            return true;
        }
        if ("23001".equals(sqlState)) {
            // H2: Unique index or primary key violation
            return true;
        }
        if ("23505".equals(sqlState)) {
            // PostgreSQL: duplicate key value violates unique constraint
            return true;
        }
        if ("S0003".equals(sqlState) || "S0005".equals(sqlState)) {
            // SQL Server: Snapshot isolation transaction aborted due to update
            // conflict
            return true;
        }
        return false;
    }

    @Override
    public Binary getBinary(String digest) {
        if (resetCache) {
            // for unit tests
            resetCache = false;
            fileCache.clear();
        }
        return super.getBinary(digest);
    }

    public class SQLFileStorage implements FileStorage {

        @Override
        public void storeFile(String digest, File file) throws IOException {
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                boolean existing;
                if (disableCheckExisting) {
                    // for unit tests
                    existing = false;
                } else {
                    logSQL(checkSql, digest);
                    PreparedStatement ps = connection.prepareStatement(checkSql);
                    ps.setString(1, digest);
                    ResultSet rs = ps.executeQuery();
                    existing = rs.next();
                    ps.close();
                }
                if (!existing) {
                    // insert new blob
                    logSQL(putSql, digest, "somebinary", Boolean.TRUE);
                    PreparedStatement ps = connection.prepareStatement(putSql);
                    ps.setString(1, digest);
                    // needs dbcp 1.4:
                    // ps.setBlob(2, new FileInputStream(file), file.length());
                    FileInputStream tmpis = new FileInputStream(file);
                    try {
                        ps.setBinaryStream(2, tmpis, (int) file.length());
                        ps.setBoolean(3, true); // mark new additions for GC
                        try {
                            ps.execute();
                        } catch (SQLException e) {
                            if (!isDuplicateKeyException(e)) {
                                throw e;
                            }
                        }
                    } finally {
                        IOUtils.closeQuietly(tmpis);
                    }
                    ps.close();
                }
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
            }
        }

        @Override
        public boolean fetchFile(String digest, File tmp) throws IOException {
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                logSQL(getSql, digest);
                PreparedStatement ps = connection.prepareStatement(getSql);
                ps.setString(1, digest);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    log.error("Unknown binary: " + digest);
                    return false;
                }
                InputStream in = rs.getBinaryStream(1);
                OutputStream out = null;
                try {
                    if (in == null) {
                        log.error("Missing binary: " + digest);
                        return false;
                    }
                    // store in file
                    out = new FileOutputStream(tmp);
                    IOUtils.copy(in, out);
                } finally {
                    IOUtils.closeQuietly(in);
                    IOUtils.closeQuietly(out);
                }
                return true;
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
            }
        }

        @Override
        public Long fetchLength(String digest) throws IOException {
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                logSQL(getLengthSql, digest);
                PreparedStatement ps = connection.prepareStatement(getLengthSql);
                ps.setString(1, digest);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    log.error("Unknown binary: " + digest);
                    return null;
                }
                return Long.valueOf(rs.getLong(1));
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
            }
        }
    }

    public static class SQLBinaryGarbageCollector implements BinaryGarbageCollector {

        protected final SQLBinaryManager binaryManager;

        protected volatile long startTime;

        protected BinaryManagerStatus status;

        public SQLBinaryGarbageCollector(SQLBinaryManager binaryManager) {
            this.binaryManager = binaryManager;
        }

        @Override
        public String getId() {
            return "datasource:" + binaryManager.dataSourceName;
        }

        @Override
        public BinaryManagerStatus getStatus() {
            return status;
        }

        @Override
        public boolean isInProgress() {
            // volatile as this is designed to be called from another thread
            return startTime != 0;
        }

        @Override
        public void start() {
            if (startTime != 0) {
                throw new RuntimeException("Already started");
            }
            startTime = System.currentTimeMillis();
            status = new BinaryManagerStatus();

            Connection connection = null;
            PreparedStatement ps = null;
            try {
                connection = binaryManager.dataSource.getConnection();
                logSQL(binaryManager.gcStartSql, Boolean.FALSE);
                ps = connection.prepareStatement(binaryManager.gcStartSql);
                ps.setBoolean(1, false); // clear marks
                int n = ps.executeUpdate();
                logSQL("  -> ? rows", Long.valueOf(n));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
            }
        }

        @Override
        public void mark(String digest) {
            Connection connection = null;
            PreparedStatement ps = null;
            try {
                connection = binaryManager.dataSource.getConnection();
                logSQL(binaryManager.gcMarkSql, Boolean.TRUE, digest);
                ps = connection.prepareStatement(binaryManager.gcMarkSql);
                ps.setBoolean(1, true); // mark
                ps.setString(2, digest);
                ps.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
            }
        }

        @Override
        public void stop(boolean delete) {
            if (startTime == 0) {
                throw new RuntimeException("Not started");
            }

            Connection connection = null;
            PreparedStatement ps = null;
            try {
                connection = binaryManager.dataSource.getConnection();
                // stats
                logSQL(binaryManager.gcStatsSql, Boolean.TRUE);
                ps = connection.prepareStatement(binaryManager.gcStatsSql);
                ps.setBoolean(1, true); // marked
                ResultSet rs = ps.executeQuery();
                rs.next();
                status.numBinaries = rs.getLong(1);
                status.sizeBinaries = rs.getLong(2);
                logSQL("  -> ?, ?", Long.valueOf(status.numBinaries), Long.valueOf(status.sizeBinaries));
                logSQL(binaryManager.gcStatsSql, Boolean.FALSE);
                ps.setBoolean(1, false); // unmarked
                rs = ps.executeQuery();
                rs.next();
                status.numBinariesGC = rs.getLong(1);
                status.sizeBinariesGC = rs.getLong(2);
                logSQL("  -> ?, ?", Long.valueOf(status.numBinariesGC), Long.valueOf(status.sizeBinariesGC));
                if (delete) {
                    // sweep
                    ps.close();
                    logSQL(binaryManager.gcSweepSql, Boolean.FALSE);
                    ps = connection.prepareStatement(binaryManager.gcSweepSql);
                    ps.setBoolean(1, false); // sweep unmarked
                    int n = ps.executeUpdate();
                    logSQL("  -> ? rows", Long.valueOf(n));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error(e, e);
                    }
                }
            }

            status.gcDuration = System.currentTimeMillis() - startTime;
            startTime = 0;
        }
    }

}
