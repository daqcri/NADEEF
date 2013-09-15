/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.test.core;

import org.junit.After;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.sql.SQLDialectBase;
import qa.qcri.nadeef.core.util.sql.SQLDialectFactory;
import qa.qcri.nadeef.test.NadeefTestBase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * SQL Manager test.
 */
@RunWith(Parameterized.class)
public class SQLManagerTest extends NadeefTestBase {
    public SQLManagerTest(String testConfig_) {
        super(testConfig_);
    }

    private static String testFile =
        "test*src*qa*qcri*nadeef*test*input*dumptest.csv".replace("*", File.separator);

    @Before
    public void setup() {
        Connection conn = null;
        try {
            Bootstrap.start(testConfig);
            conn = DBConnectionFactory.getNadeefConnection();
            Statement stat = conn.createStatement();

            BufferedReader reader = new BufferedReader(new FileReader(testFile));
            String line = reader.readLine();
            stat.execute("CREATE TABLE DUMPTEST (" + line + ")");
            while ((line = reader.readLine()) != null) {
                StringBuilder builder = new StringBuilder(1024);
                String[] tokens = line.split(",");
                int i = 0;
                for (String token : tokens) {
                    if (i != 0) {
                        builder.append(",");
                    }
                    builder.append("\'").append(token).append("\'");
                    i ++;
                }
                stat.addBatch("INSERT INTO DUMPTEST VALUES (" + builder.toString() + ")");
            }
            stat.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
    }

    @After
    public void shutdown() {
        Connection conn = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            Statement stat = conn.createStatement();
            stat.execute("DROP TABLE DUMPTEST");
            stat.execute("DROP TABLE DUMPTEST2");
            conn.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
            Bootstrap.shutdown();
        }
    }

    @Test
    public void copyTableTest() {
        SQLDialectBase dialect =
            SQLDialectFactory.getNadeefDialectManagerInstance();

        Connection conn = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            Statement stat = conn.createStatement();
            dialect.copyTable(conn, "DUMPTEST", "DUMPTEST2");
            conn.commit();
            Assert.assertEquals(12, getMaxTid(stat, "DUMPTEST2"));
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private int getMaxTid(Statement stat, String tableName) throws Exception {
        ResultSet rs = stat.executeQuery("SELECT * FROM " + tableName);
        int c = 0;
        int p = 0;
        while (rs.next()) {
            c = rs.getInt("tid");
            Assert.assertTrue(c - p == 1);
            p = c;
        }
        return c;
    }
}
