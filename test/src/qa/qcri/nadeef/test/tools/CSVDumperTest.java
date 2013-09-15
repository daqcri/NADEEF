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

package qa.qcri.nadeef.test.tools;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.sql.SQLDialectBase;
import qa.qcri.nadeef.core.util.sql.SQLDialectFactory;
import qa.qcri.nadeef.test.NadeefTestBase;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.core.util.CSVTools;
import qa.qcri.nadeef.tools.DBConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * CSV Dumper test.
 */
@RunWith(Parameterized.class)
public class CSVDumperTest extends NadeefTestBase {
    private static String tableName;
    private static Connection conn;
    private static SQLDialectBase dialectManager;

    public CSVDumperTest(String config) {
        super(config);
    }

    @Before
    public void setUp() {
        try {
           Bootstrap.start(testConfig);
           DBConfig dbConfig = NadeefConfiguration.getDbConfig();
           dialectManager =
               SQLDialectFactory.getDialectManagerInstance(dbConfig.getDialect());
           conn =
               DBConnectionFactory.createConnection(dbConfig);
           conn.setAutoCommit(false);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @After
    public void tearDown() {
        Statement stat = null;
        if (conn != null) {
            try {
                stat = conn.createStatement();
                stat.execute(dialectManager.dropTable(tableName));
                conn.commit();
            } catch (Exception ex) {
                Assert.fail(ex.getMessage());
            } finally {
                try {
                    if (stat != null) {
                        stat.close();
                    }

                    conn.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
    }

    @Test
    public void goodDumpTest() {
        Statement stat = null;
        try {
            tableName =
                CSVTools.dump(conn, dialectManager, TestDataRepository.getDumpTestCSVFile());
            Assert.assertNotNull("tableName cannot be null", tableName);

            BufferedReader reader =
                new BufferedReader(new FileReader(TestDataRepository.getDumpTestCSVFile()));
            int lineCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                lineCount ++;
            }
            reader.close();

            stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
            int rowCount = -1;
            if (resultSet.next()) {
                rowCount = resultSet.getInt(1);
            }

            resultSet.close();
            Assert.assertEquals("Row number is not correct", lineCount - 1, rowCount);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    Assert.fail(e.getMessage());
                }
            }
        }
    }
}
