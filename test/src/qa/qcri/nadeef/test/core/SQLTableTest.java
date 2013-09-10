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

import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.sql.NadeefSQLDialectManagerBase;
import qa.qcri.nadeef.core.util.sql.SQLDialectManagerFactory;
import qa.qcri.nadeef.test.NadeefTestBase;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.CSVTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SQLDialect;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * SourceDeserializer Test.
 */
@RunWith(Parameterized.class)
public class SQLTableTest extends NadeefTestBase {
    private String tableName;
    private DBConfig dbconfig;
    private DBConnectionFactory connectionFactory;

    public SQLTableTest(String config_) {
        super(config_);
    }

    @Before
    public void setup() {
        Connection conn = null;
        try {
            Bootstrap.start(testConfig);
            dbconfig =
                new DBConfig.Builder()
                    .dialect(SQLDialect.DERBY)
                    .url("memory:test;create=true")
                    .build();
            NadeefSQLDialectManagerBase dialectManager =
                    SQLDialectManagerFactory.getDialectManagerInstance(dbconfig.getDialect());
            conn = DBConnectionFactory.createConnection(dbconfig);
            tableName =
                CSVTools.dump(conn, dialectManager, TestDataRepository.getDumpTestCSVFile());
            connectionFactory = DBConnectionFactory.createDBConnectionFactory(dbconfig);
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
    public void teardown() {
        Connection conn = null;
        try {
            conn = connectionFactory.getSourceConnection();
            NadeefSQLDialectManagerBase dialectManager =
                SQLDialectManagerFactory.getNadeefDialectManagerInstance();
            Statement stat = conn.createStatement();
            stat.execute(dialectManager.dropTable(tableName));
            conn.commit();
            Bootstrap.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
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

    @Test
    public void testProjection() {
        SQLTable collection = new SQLTable(tableName, connectionFactory);
        collection.project(new Column(tableName + ".c"));
        Assert.assertEquals(12, collection.size());
        Tuple tuple = collection.get(0);
        Set cellSets = tuple.getCells();
        Assert.assertEquals(1, cellSets.size());
        Cell cell = (Cell)cellSets.iterator().next();
        Assert.assertTrue(
                (tableName + ".c").equalsIgnoreCase(cell.getColumn().getFullColumnName())
        );
    }

    @Test
    public void testFilter() {
        SQLTable collection = new SQLTable(tableName, connectionFactory);
        collection.filter(SimpleExpression.newEqual(new Column(tableName, "c"), "c1"))
                .project(new Column(tableName, "c"));
        Assert.assertEquals(7, collection.size());
        Tuple tuple = collection.get(0);
        Set cellSets = tuple.getCells();
        Assert.assertEquals(1, cellSets.size());
        Cell cell = (Cell)cellSets.iterator().next();
        Assert.assertTrue(
            (tableName + ".c").equalsIgnoreCase(cell.getColumn().getFullColumnName()));
    }

    @Test
    public void testGroup() {
        SQLTable collection = new SQLTable(tableName, connectionFactory);
        Column targetColumn = new Column(tableName, "c");
        Collection<Table> result = collection.groupOn(targetColumn);
        Assert.assertEquals(3, result.size());
        for (Table t : result) {
            Tuple tuple = t.get(0);
            String value =(String) tuple.get("c");
            switch (value) {
                case "c1":
                    Assert.assertEquals(7, t.size());
                    break;
                case "c3":
                    Assert.assertEquals(1, t.size());
                    break;
                case "c2":
                    Assert.assertEquals(4, t.size());
                    break;
            }

        }
    }

    // @Test
    public void testSize() throws InterruptedException {
        Stopwatch stopwatch = new Stopwatch().start();
        SQLTable table = new SQLTable("csv_test60m", connectionFactory);
        table.get(0);

        long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        int mb = 1024*1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        System.out.println("##### Heap utilization statistics [MB] #####");

        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / mb;
        System.out.println("Used Memory:" + usedMemory);
        System.out.println("Free Memory:" + runtime.freeMemory() / mb);
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
        System.out.println("Load time: " + elapsedTime + " ms.");

        // memory usage should be less than 700 mb.
        Assert.assertTrue(usedMemory < 700);

        // load time should be less than 18000 ms
        Assert.assertTrue(elapsedTime < 18000);
    }
}
