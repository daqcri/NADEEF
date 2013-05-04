/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.CSVDumper;

import java.sql.Connection;
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
        Connection conn = null;
        try {
            Bootstrap.Start();
            DBConfig.Builder builder = new DBConfig.Builder();
            dbconfig =
                builder.url("localhost/unittest")
                       .username("tester")
                       .password("tester")
                       .dialect(SQLDialect.POSTGRES)
                       .build();

            conn = DBConnectionFactory.createConnection(dbconfig);
            tableName = CSVDumper.dump(conn, TestDataRepository.getDumpTestCSVFile());
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testProjection() {
        SQLTupleCollection collection = new SQLTupleCollection(tableName, dbconfig);
        collection.project(new Column(tableName + ".c"));
        Assert.assertEquals(12, collection.size());
        Tuple tuple = collection.get(0);
        Set cellSets = tuple.getCells();
        Assert.assertEquals(1, cellSets.size());
        Cell cell = (Cell)cellSets.iterator().next();
        Assert.assertEquals(tableName + ".c", cell.getColumn().getFullAttributeName());
    }

    @Test
    public void testFilter() {
        SQLTupleCollection collection = new SQLTupleCollection(tableName, dbconfig);
        collection.filter(SimpleExpression.newEqual(new Column(tableName, "c"), "c1"))
                .project(new Column(tableName, "c"));
        Assert.assertEquals(7, collection.size());
        Tuple tuple = collection.get(0);
        Set cellSets = tuple.getCells();
        Assert.assertEquals(1, cellSets.size());
        Cell cell = (Cell)cellSets.iterator().next();
        Assert.assertEquals(tableName + ".c", cell.getColumn().getFullAttributeName());
    }

    @Test
    public void testGroup() {
        SQLTupleCollection collection = new SQLTupleCollection(tableName, dbconfig);
        Column targetColumn = new Column(tableName, "c");
        Collection<TupleCollection> result = collection.groupOn(targetColumn);
        Assert.assertEquals(3, result.size());
        for (TupleCollection t : result) {
            Tuple tuple = t.get(0);
            String value =(String) tuple.get(targetColumn);
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
