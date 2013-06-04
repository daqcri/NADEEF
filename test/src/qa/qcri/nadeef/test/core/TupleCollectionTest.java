/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.CSVDumper;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SQLDialect;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.Set;

/**
 * SourceDeserializer Test.
 */
@RunWith(JUnit4.class)
public class TupleCollectionTest {
    private String tableName;
    private DBConfig dbconfig;

    @Before
    public void setup() {
        Bootstrap.start();
        Connection conn = null;
        try {
            Bootstrap.start();
            DBConfig.Builder builder = new DBConfig.Builder();
            dbconfig =
                builder.url("localhost/unittest")
                       .username("tester")
                       .password("tester")
                       .dialect(SQLDialect.POSTGRES)
                       .build();
            DBConnectionFactory.initializeSource(dbconfig);
            conn = DBConnectionFactory.createConnection(dbconfig);
            tableName = CSVDumper.dump(conn, TestDataRepository.getDumpTestCSVFile());
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
            conn = DBConnectionFactory.getNadeefConnection();
            Statement stat = conn.createStatement();
            stat.execute("DROP TABLE " + tableName + " CASCADE");
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
        SQLTable collection = new SQLTable(tableName, dbconfig);
        collection.project(new Column(tableName + ".c"));
        Assert.assertEquals(12, collection.size());
        Tuple tuple = collection.get(0);
        Set cellSets = tuple.getCells();
        Assert.assertEquals(1, cellSets.size());
        Cell cell = (Cell)cellSets.iterator().next();
        Assert.assertEquals(tableName + ".c", cell.getColumn().getFullColumnName());
    }

    @Test
    public void testFilter() {
        SQLTable collection = new SQLTable(tableName, dbconfig);
        collection.filter(SimpleExpression.newEqual(new Column(tableName, "c"), "c1"))
                .project(new Column(tableName, "c"));
        Assert.assertEquals(7, collection.size());
        Tuple tuple = collection.get(0);
        Set cellSets = tuple.getCells();
        Assert.assertEquals(1, cellSets.size());
        Cell cell = (Cell)cellSets.iterator().next();
        Assert.assertEquals(tableName + ".c", cell.getColumn().getFullColumnName());
    }

    @Test
    public void testGroup() {
        SQLTable collection = new SQLTable(tableName, dbconfig);
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
}
