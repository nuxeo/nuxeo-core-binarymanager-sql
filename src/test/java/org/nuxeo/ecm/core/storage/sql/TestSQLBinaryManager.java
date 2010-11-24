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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;

import javax.sql.DataSource;

import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.impl.DocumentModelImpl;
import org.nuxeo.ecm.core.api.impl.blob.ByteArrayBlob;
import org.nuxeo.runtime.AbstractRuntimeService;
import org.nuxeo.runtime.api.DataSourceHelper;
import org.nuxeo.runtime.api.Framework;

/*
 * Note that this unit test cannot be run with Nuxeo 5.4.0 (NXP-6021 needed).
 */
public class TestSQLBinaryManager extends SQLRepositoryTestCase {

    private static final String CONTENT = "this is a file";

    private static final String CONTENT_MD5 = "139ec4f94a8c908e20e7c2dce5092af4";

    /** This directory will be deleted and recreated. */
    private static final String H2_DIR = "target/test/h2-ds";

    /** Property used in the datasource URL. */
    private static final String H2_URL_PROP = "nuxeo.test.ds.url";

    // also in datasource-*-contrib.xml and repo-*-contrib.xml
    public static final String DATASOURCE = "jdbc/binaries";

    // also in repo-*-contrib.xml
    public static final String TABLE = "binaries";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String db = database.getClass().getSimpleName().toLowerCase();
        if (db.startsWith("database")) {
            db = db.substring("database".length());
        }

        if (database instanceof DatabaseH2) {
            File h2dir = new File(H2_DIR);
            FileUtils.deleteTree(h2dir);
            h2dir.mkdirs();
            File h2db = new File(h2dir, "binaries");
            String h2Url = "jdbc:h2:" + h2db.getAbsolutePath();
            ((AbstractRuntimeService) runtime).setProperty(H2_URL_PROP, h2Url);
        }
        deployBundle("org.nuxeo.runtime.datasource");
        deployContrib("org.nuxeo.ecm.core.storage.binarymanager.sql.tests",
                String.format("OSGI-INF/datasource-%s-contrib.xml", db));

        deployContrib("org.nuxeo.ecm.core.storage.binarymanager.sql.tests",
                String.format("OSGI-INF/repo-%s-contrib.xml", db));
        openSession();

        // create table in database
        DataSource dataSource = DataSourceHelper.getDataSource(DATASOURCE);
        Connection connection = dataSource.getConnection();
        Statement st = connection.createStatement();
        String blobType;
        if (database instanceof DatabaseH2) {
            blobType = "BLOB";
        } else if (database instanceof DatabasePostgreSQL) {
            blobType = "BYTEA";
        } else if (database instanceof DatabaseMySQL) {
            blobType = "BLOB";
        } else if (database instanceof DatabaseOracle) {
            blobType = "BLOB";
        } else if (database instanceof DatabaseSQLServer) {
            blobType = "VARBINARY(MAX)";
        } else {
            fail("Database " + database.getClass().getSimpleName() + " TODO");
            return;
        }
        String sql = String.format(
                "CREATE TABLE %s (%s VARCHAR(256) PRIMARY KEY, %s %s)", TABLE,
                SQLBinaryManager.COL_ID, SQLBinaryManager.COL_BIN, blobType);
        st.execute(sql);
        connection.close();
    }

    @Override
    public void tearDown() throws Exception {
        if (database instanceof DatabaseH2) {
            String url = Framework.getProperty(H2_URL_PROP);
            Connection connection = DriverManager.getConnection(url);
            Statement st = connection.createStatement();
            st.execute("SHUTDOWN");
            st.close();
            connection.close();
        }
        session.cancel();
        closeSession();
        super.tearDown();
    }

    public void testSQLBinaryManager() throws Exception {
        DocumentModel file = new DocumentModelImpl("/", "myfile", "File");
        file = session.createDocument(file);
        session.save();

        byte[] bytes = CONTENT.getBytes("UTF-8");
        Blob blob = new ByteArrayBlob(bytes, "application/octet-stream",
                "UTF-8");
        blob.setFilename("blob.txt");

        file.setProperty("file", "content", blob);
        session.saveDocument(file);
        session.save();

        file = session.getDocument(file.getRef());
        blob = (Blob) file.getPropertyValue("file:content");

        assertEquals("blob.txt", blob.getFilename());
        assertEquals(bytes.length, blob.getLength());
        assertTrue(Arrays.equals(bytes, blob.getByteArray()));

        // actually fetch from database
        SQLBinaryManager.resetCache = true;
        closeSession();
        openSession();
        file = session.getDocument(file.getRef());
        blob = (Blob) file.getPropertyValue("file:content");
    }

    public void testSQLBinaryManagerDuplicate() throws Exception {
        DocumentModel file = new DocumentModelImpl("/", "myfile", "File");
        file = session.createDocument(file);
        session.save();

        byte[] bytes = CONTENT.getBytes("UTF-8");

        // pre-create value in table to force collision
        DataSource dataSource = DataSourceHelper.getDataSource(DATASOURCE);
        Connection connection = dataSource.getConnection();
        String sql = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)",
                TABLE, SQLBinaryManager.COL_ID, SQLBinaryManager.COL_BIN);
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, CONTENT_MD5);
        ps.setBinaryStream(2, new ByteArrayInputStream(bytes), bytes.length);
        ps.execute();
        connection.close();

        // don't do collision checks to provoke insert collision
        SQLBinaryManager.disableCheckExisting = true;

        Blob blob = new ByteArrayBlob(bytes, "application/octet-stream",
                "UTF-8");
        file.setProperty("file", "content", blob);
        session.saveDocument(file);
        session.save();
    }

}
