/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.file.FileCache;
import org.nuxeo.common.file.LRUFileCache;
import org.nuxeo.ecm.core.storage.StorageException;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Column;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Database;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Table;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.Dialect;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.DialectH2;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.DialectOracle;
import org.nuxeo.ecm.core.storage.sql.jdbc.dialect.DialectSQLServer;
import org.nuxeo.runtime.api.DataSourceHelper;

/**
 * A Binary Manager that stores binaries as SQL BLOBs.
 * <p>
 * The BLOBs are cached locally on first access for efficiency.
 * <p>
 * Because the BLOB length can be accessed independently of the binary stream,
 * it is also cached in a simple text file if accessed before the stream.
 */
public class SQLBinaryManager extends DefaultBinaryManager {

    private static final Log log = LogFactory.getLog(SQLBinaryManager.class);

    public static final String DS_PREFIX = "datasource=";

    public static final String TABLE_PREFIX = "table=";

    public static final String CACHE_SIZE_PREFIX = "cachesize=";

    public static final String DEFAULT_CACHE_SIZE = "10M";

    public static final String COL_ID = "id";

    public static final String COL_BIN = "bin";

    protected static final String LEN_DIGEST_SUFFIX = "-len";

    protected DataSource dataSource;

    protected FileCache fileCache;

    protected String checkSql;

    protected String putSql;

    protected String getSql;

    protected String getLengthSql;

    // Used just to pass to Binary constructor TODO refactor Binary
    protected File dummyFile;

    protected static boolean disableCheckExisting; // for unit tests

    protected static boolean resetCache; // for unit tests

    @Override
    public void initialize(RepositoryDescriptor repositoryDescriptor)
            throws IOException {
        repositoryName = repositoryDescriptor.name;
        descriptor = new BinaryManagerDescriptor();
        descriptor.digest = getDigest();
        log.info("Repository '" + repositoryDescriptor.name + "' using "
                + this.getClass().getSimpleName());

        String dataSourceName = null;
        String tableName = null;
        String cacheSizeStr = DEFAULT_CACHE_SIZE;
        for (String part : repositoryDescriptor.binaryManagerKey.split(",")) {
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
        if (dataSourceName == null) {
            throw new RuntimeException("Missing " + DS_PREFIX
                    + " in binaryManager key");
        }
        if (tableName == null) {
            throw new RuntimeException("Missing " + TABLE_PREFIX
                    + " in binaryManager key");
        }

        try {
            dataSource = DataSourceHelper.getDataSource(dataSourceName);
        } catch (NamingException e) {
            throw new IOException("Cannot find datasource: " + dataSourceName,
                    e);
        }

        // create the SQL statements used
        createSql(tableName);

        // create file cache
        File dir = File.createTempFile("nxbincache.", "", null);
        dir.delete();
        dir.mkdir();
        dir.deleteOnExit();
        long cacheSize = parseSizeInBytes(cacheSizeStr);
        fileCache = new LRUFileCache(dir, cacheSize);
        log.info("Using binary cache directory: " + dir.getPath() + " size: "
                + cacheSizeStr);

        // TODO refactor Binary
        dummyFile = File.createTempFile("nxempty", null);
        dummyFile.deleteOnExit();
    }

    protected void createSql(String tableName) throws IOException {
        Dialect dialect = getDialect();
        Database database = new Database(dialect);
        Table table = database.addTable(tableName);
        ColumnType dummytype = ColumnType.STRING;
        Column idCol = table.addColumn(COL_ID, dummytype, COL_ID, null);
        Column binCol = table.addColumn(COL_BIN, dummytype, COL_BIN, null);

        checkSql = String.format("SELECT 1 FROM %s WHERE %s = ?",
                table.getQuotedName(), idCol.getQuotedName());
        putSql = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)",
                table.getQuotedName(), idCol.getQuotedName(),
                binCol.getQuotedName());
        getSql = String.format("SELECT %s FROM %s WHERE %s = ?",
                binCol.getQuotedName(), table.getQuotedName(),
                idCol.getQuotedName());
        // TODO move this to Dialect
        String lengthFunc;
        if (dialect instanceof DialectOracle) {
            lengthFunc = "LENGTHB";
        } else if (dialect instanceof DialectSQLServer) {
            lengthFunc = "DATALENGTH";
        } else if (dialect instanceof DialectH2) {
            lengthFunc = "LENGTH";
        } else { // PostgreSQL, MySQL
            lengthFunc = "OCTET_LENGTH";
        }
        getLengthSql = String.format("SELECT %s(%s) FROM %s WHERE %s = ?",
                lengthFunc, binCol.getQuotedName(), table.getQuotedName(),
                idCol.getQuotedName());
    }

    protected Dialect getDialect() throws IOException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            return Dialect.createDialect(connection, null, null);
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

    protected long parseSizeInBytes(String string) {
        String digits = string;
        if (digits.length() == 0) {
            throw new RuntimeException("Invalid empty size");
        }
        char unit = digits.charAt(digits.length() - 1);
        if (unit == 'b' || unit == 'B') {
            digits = digits.substring(0, digits.length() - 1);
            if (digits.length() == 0) {
                throw new RuntimeException("Invalid size: '" + string + "'");
            }
            unit = digits.charAt(digits.length() - 1);
        }
        long mul;
        switch (unit) {
        case 'k':
        case 'K':
            mul = 1024;
            break;
        case 'm':
        case 'M':
            mul = 1024 * 1024;
            break;
        case 'g':
        case 'G':
            mul = 1024 * 1024 * 1024;
            break;
        default:
            if (!Character.isDigit(unit)) {
                throw new RuntimeException("Invalid size: '" + string + "'");
            }
            mul = 1;
        }
        if (mul != 1) {
            digits = digits.substring(0, digits.length() - 1);
            if (digits.length() == 0) {
                throw new RuntimeException("Invalid size: '" + string + "'");
            }
        }
        try {
            return Long.parseLong(digits) * mul;
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid size: '" + string + "'");
        }
    }

    /**
     * Gets the message digest to use to hash binaries.
     */
    protected String getDigest() {
        return DEFAULT_DIGEST;
    }

    @Override
    public Binary getBinary(InputStream in) throws IOException {
        // write the input stream to a temporary file, while computing a digest
        File tmp = fileCache.getTempFile();
        OutputStream out = new FileOutputStream(tmp);
        String digest;
        try {
            digest = storeAndDigest(in, out);
        } finally {
            in.close();
            out.close();
        }

        // register the file in the file cache
        File file = fileCache.putFile(digest, tmp);

        // store the blob in the SQL database
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            boolean existing;
            if (disableCheckExisting) {
                // for unit tests
                existing = false;
            } else {
                PreparedStatement ps = connection.prepareStatement(checkSql);
                ps.setString(1, digest);
                ResultSet rs = ps.executeQuery();
                existing = rs.next();
                ps.close();
            }
            if (!existing) {
                // insert new blob
                PreparedStatement ps = connection.prepareStatement(putSql);
                ps.setString(1, digest);
                // needs dbcp 1.4:
                // ps.setBlob(2, new FileInputStream(file), file.length());
                ps.setBinaryStream(2, new FileInputStream(file),
                        (int) file.length());
                try {
                    ps.execute();
                } catch (SQLException e) {
                    if (!isDuplicateKeyException(e)) {
                        throw e;
                    }
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
        return new Binary(file, digest, repositoryName);
    }

    protected boolean isDuplicateKeyException(SQLException e) {
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
        return false;
    }

    @Override
    public Binary getBinary(String digest) {
        if (resetCache) {
            // for unit tests
            resetCache = false;
            fileCache.clear();
        }
        // check in the cache
        File file = fileCache.getFile(digest);
        if (file == null) {
            return new SQLLazyBinary(digest);
        } else {
            return new Binary(file, digest, repositoryName);
        }
    }

    /**
     * A lazy Binary that fetches its remote stream on first access.
     */
    public class SQLLazyBinary extends Binary {

        private static final long serialVersionUID = 1L;

        protected Long sqlLength;

        protected File sqlFile;

        public SQLLazyBinary(String digest) {
            super(dummyFile, digest);
        }

        @Override
        public InputStream getStream() throws IOException {
            if (sqlFile == null) {
                sqlFile = fetchFile();
                if (sqlFile != null) {
                    sqlLength = Long.valueOf(sqlFile.length());
                }
            }
            return sqlFile == null ? null : new FileInputStream(sqlFile);
        }

        /**
         * Fetches the file from the cache or the remote database.
         * <p>
         * If something is retrieved from the database, it is put in cache.
         *
         * @return the blob, or {@code null} if the expected blob is missing
         *         from the database.
         */
        protected File fetchFile() {
            String digest = getDigest();

            File f = fileCache.getFile(digest);
            if (f != null) {
                return f;
            }

            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(getSql);
                ps.setString(1, digest);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    log.error("Unknown binary: " + digest);
                    return null;
                }
                InputStream in = rs.getBinaryStream(1);
                if (in == null) {
                    log.error("Missing binary: " + digest);
                    return null;
                }
                OutputStream out = null;
                // store in file
                File tmp;
                try {
                    tmp = fileCache.getTempFile();
                    out = new FileOutputStream(tmp);
                    IOUtils.copy(in, out);
                } finally {
                    in.close();
                    if (out != null) {
                        out.close();
                    }
                }
                // register the file in the file cache
                return fileCache.putFile(digest, tmp);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
        public long getLength() {
            if (sqlLength == null) {
                sqlLength = fetchLength();
            }
            return sqlLength == null ? 0 : sqlLength.longValue();
        }

        /**
         * Fetches the length from the cache or the remote database.
         * <p>
         * If something is retrieved from the database, it is put in cache.
         *
         * @return the length, or {@code null} if the expected blob is missing
         *         from the database.
         */
        protected Long fetchLength() {
            Long len = getLengthFromCache();
            if (len != null) {
                return len;
            }

            String digest = getDigest();
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(getLengthSql);
                ps.setString(1, digest);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    log.error("Unknown binary: " + digest);
                    return null;
                }
                len = Long.valueOf(rs.getLong(1));
                putLengthInCache(len);
                return len;
            } catch (SQLException e) {
                throw new RuntimeException(e);
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

        protected Long getLengthFromCache() {
            File f = fileCache.getFile(getDigest() + LEN_DIGEST_SUFFIX);
            if (f == null) {
                return null;
            }
            // read decimal length from file
            InputStream in = null;
            try {
                in = new FileInputStream(f);
                String len = IOUtils.toString(in);
                return Long.valueOf(len);
            } catch (Exception e) {
                log.error(e, e);
                return null;
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        log.error(e, e);
                    }
                }
            }

        }

        protected void putLengthInCache(Long len) {
            OutputStream out = null;
            try {
                File tmp = fileCache.getTempFile();
                out = new FileOutputStream(tmp);
                Writer writer = new OutputStreamWriter(out);
                writer.write(len.toString());
                writer.flush();
                out.close();
                out = null;
                fileCache.putFile(getDigest() + LEN_DIGEST_SUFFIX, tmp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        log.error(e, e);
                    }
                }
            }
        }
    }

}
