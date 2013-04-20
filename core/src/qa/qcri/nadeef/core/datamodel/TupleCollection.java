/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.core.util.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Tuple collection class.
 */
public class TupleCollection {
    private DBConfig dbconfig;
    private String tableName;
    private SqlQueryBuilder sqlQuery;
    private ArrayList<Tuple> tuples;

    private static Tracer tracer = Tracer.getTracer(TupleCollection.class);

    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     * @param collection a collection of <code>Tuples</code>, by
     *                   using this constructor the result <code>TupleCollection</code>
     *                   will be an orphan collection (no database connection behind).
     */
    public TupleCollection(Collection<Tuple> collection) {
        tuples = Lists.newArrayList(collection);
    }

    /**
     * Constructor with database connection.
     * @param tableName tuple collection table name.
     * @param dbconfig used database connection.
     */
    public TupleCollection(String tableName, DBConfig dbconfig)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        Preconditions.checkNotNull(dbconfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
        this.dbconfig = dbconfig;
        this.tableName = tableName;
        this.sqlQuery = new SqlQueryBuilder();
        this.sqlQuery.addFrom(tableName);
    }
    //</editor-fold>

    /**
     * Synchronize the collection data with the underlying database.
     * @return Returns <code>True</code> when the synchronization is successful.
     */
    public synchronized boolean sync()
        throws SQLException,
               InstantiationException,
               IllegalAccessException,
               ClassNotFoundException {
        if (isOrphan()) {
            tracer.info("TupleCollection is an orphan, sync failed.");
            return false;
        }

        tuples = new ArrayList();
        Connection conn = DBConnectionFactory.createConnection(dbconfig);
        Statement stat = conn.createStatement();
        ResultSet resultSet = stat.executeQuery(sqlQuery.toSQLString());
        conn.commit();
        int tupleId = 1;
        while(resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            Cell[] cells = new Cell[count];
            Object[] values = new Object[count];
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                if (attributeName.equalsIgnoreCase("tid")) {
                    tupleId = (int)resultSet.getObject(i);
                }
                String tableName =
                    DBMetaDataTool.getBaseTableName(dbconfig.getDialect(), metaData, i);
                cells[i - 1] = new Cell(tableName, attributeName);
                values[i - 1] = resultSet.getObject(i);
            }

            tuples.add(new Tuple(tupleId, cells, values));
            tupleId ++;
        }
        stat.close();
        conn.close();
        return true;
    }

    /**
     * Gets the SQL query of this tuple collection.
     * @return <code>SqlQueryBuilder</code> instance.
     */
    public SqlQueryBuilder getSQLQuery() {
        return sqlQuery;
    }

    /**
     * Gets the size of the collection.
     * It will call <code>sync</code> if the collection is not yet existed.
     *
     * @return size of the collection.
     */
    public int size() {
        try {
            if (tuples == null) {
                sync();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return 0;
        }
         return tuples.size();
    }

    /**
     * Gets the tuple from the collection.
     * @param i tuple index.
     * @return tuple instance.
     */
    public Tuple get(int i) {
        try {
            if (tuples == null) {
                sync();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

        return tuples.get(i);
    }

    /**
     * Gets the DB config.
     * @return DB config.
     */
    public DBConfig getDbconfig() {
        return this.dbconfig;
    }

    /**
     * Return <code>True</code> when the tuple collection has no Database underneath.
     * @return
     */
    public boolean isOrphan() {
        return dbconfig == null;
    }

    /**
     * Custom equals compare.
     * @param collection target collection.
     * @return Returns <code>True</code> if the given collection is the same.
     */
    @Override
    public boolean equals(Object collection) {
        if (collection == this) {
            return true;
        }

        if (collection == null || !(collection instanceof TupleCollection)) {
            return false;
        }

        TupleCollection obj = (TupleCollection)collection;
        if (dbconfig.equals(obj.dbconfig) && tableName.equals(obj.tableName)) {
            return true;
        }

        return this.tuples.equals(obj.tuples);
    }
}
