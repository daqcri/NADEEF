/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;


/**
 * Tuple collection class.
 */
public class SQLTupleCollection extends TupleCollection {
    private DBConfig dbconfig;
    private String tableName;
    private SqlQueryBuilder sqlQuery;
    private ArrayList<Tuple> tuples;
    // indicate whether this tuple collection is an internal collection which needs to be removed
    // after finalize.
    private boolean isInternal;

    private static Tracer tracer = Tracer.getTracer(SQLTupleCollection.class);


    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     * @param collection a collection of <code>Tuples</code>, by
     *                   using this constructor the result <code>TupleCollection</code>
     *                   will be an orphan collection (no database connection behind).
     */
    public SQLTupleCollection(Collection<Tuple> collection) {
        super(null);
        if (collection.size() <= 0) {
            throw new IllegalArgumentException("Input collection cannot be empty.");
        }

        tuples = Lists.newArrayList(collection);
        schema = tuples.get(0).getSchema();
        dbconfig = null;
    }

    /**
     * Constructor with database connection.
     * @param tableName tuple collection table name.
     * @param dbconfig used database connection.
     */
    public SQLTupleCollection(String tableName, DBConfig dbconfig) {
        super(null);
        this.dbconfig = Preconditions.checkNotNull(dbconfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
        this.tableName = tableName;
        this.sqlQuery = new SqlQueryBuilder();
        this.sqlQuery.addFrom(tableName);
    }

    //</editor-fold>

    //<editor-fold desc="TupleCollection Interface">

    /**
     * Creates a tuple collection from a collection of tuples.
     *
     * @param tuples a collection of tuples.
     * @return <code>TupleCollection</code> instance.
     */
    @Override
    protected TupleCollection newTupleCollection(Collection<Tuple> tuples) {
        return new SQLTupleCollection(tuples);
    }

    /**
     * Gets the size of the collection.
     * It will call <code>sync</code> if the collection is not yet existed.
     *
     * @return size of the collection.
     */
    @Override
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

    @Override
    public Schema getSchema() {
        // TODO: write synchronization on SqlQueryBuilder
        try {
            SqlQueryBuilder builder = new SqlQueryBuilder(sqlQuery);
            builder.setLimit(1);
            Connection conn = DBConnectionFactory.createConnection(dbconfig);
            ResultSet resultSet = conn.createStatement().executeQuery(builder.build());
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            List<Column> columns = new ArrayList<Column>();
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                columns.add(new Column(tableName, attributeName));
            }

            schema = new Schema(tableName, columns);
            conn.close();
        } catch (Exception ex) {
            tracer.err("Cannot get valid schema.");
            ex.printStackTrace();
        }
        return schema;
    }

    /**
     * Sets whether it is an internal table / view.
     * @param isInternal
     */
    public void setInternal(boolean isInternal) {
        this.isInternal = isInternal;
    }

    /**
     * Gets the tuple from the collection.
     * @param i tuple index.
     * @return tuple instance.
     */
    @Override
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

    @Override
    public TupleCollection project(Column column) {
        sqlQuery.addSelect(column.getFullAttributeName());
        return this;
    }

    @Override
    public TupleCollection project(Collection<Column> columns) {
        for (Column column : columns) {
            sqlQuery.addSelect(column.getFullAttributeName());
        }
        return this;
    }

    @Override
    public TupleCollection orderBy(Column column) {
        sqlQuery.addOrder(column.getFullAttributeName());
        return this;
    }

    @Override
    public TupleCollection orderBy(Collection<Column> columns) {
        for (Column column : columns) {
            sqlQuery.addSelect(column.getFullAttributeName());
        }
        return this;
    }

    @Override
    public TupleCollection filter(SimpleExpression expression) {
        sqlQuery.addWhere(expression.toString());
        return this;
    }

    @Override
    public TupleCollection filter(List<SimpleExpression> expressions) {
        for (SimpleExpression expression : expressions) {
            sqlQuery.addWhere(expression.toString());
        }
        return this;
    }

    @Override
    public synchronized Collection<TupleCollection> groupOn(Column column) {
        Collection<TupleCollection> result = null;
        if (isOrphan()) {
            result = super.groupOn(column);
        } else {
            result = Lists.newArrayList();
            Connection conn = null;
            try {
                String sql =
                    "SELECT DISTINCT(" + column.getFullAttributeName() + ") FROM " + tableName;
                conn = DBConnectionFactory.createConnection(dbconfig);
                Statement stat = conn.createStatement();
                ResultSet distinctResult = stat.executeQuery(sql);
                Statement viewStat = conn.createStatement();
                while (distinctResult.next()) {
                    String newTableName = "VIEW" + UUID.randomUUID().toString().replace("-", "");
                    String value = distinctResult.getObject(1).toString();
                    if (!value.matches("^[0-9]+$")) {
                        value = '\'' + value + '\'';
                    }

                    sql =
                        "CREATE VIEW " +
                            newTableName + " AS " +
                            "SELECT * FROM " +
                            tableName + " WHERE " +
                            column.getFullAttributeName() + " = " +
                            value.toString();
                    tracer.verbose("Group on " + sql);
                    viewStat.execute(sql);
                    SQLTupleCollection newTupleCollection =
                        new SQLTupleCollection(newTableName, dbconfig);
                    newTupleCollection.setInternal(true);
                    result.add(newTupleCollection);
                }
                conn.commit();
            } catch (Exception ex) {
                ex.printStackTrace();
                tracer.err(ex.getMessage());
                // as a backup plan we try to use in-memory solution.
                result = super.groupOn(column);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }
        return result;
    }

    //</editor-fold>

    //<editor-fold desc="Equalization Interface">
    /**
     * Custom equals compare.
     * @param collection target collection.
     * @return Returns <code>True</code> if the given collection is the same.
     */
    // TODO: write hashcode override.
    @Override
    public boolean equals(Object collection) {
        if (collection == this) {
            return true;
        }

        if (collection == null || !(collection instanceof SQLTupleCollection)) {
            return false;
        }

        SQLTupleCollection obj = (SQLTupleCollection)collection;
        if (dbconfig.equals(obj.dbconfig) && tableName.equals(obj.tableName)) {
            return true;
        }

        return this.tuples.equals(obj.tuples);
    }
    //</editor-fold>

    //<editor-fold desc="Private members">
    /**
     * Return <code>True</code> when the tuple collection
     *        has no Database underneath.
     * @return <code>True</code> when the tuple collection
     *          has no Database underneath.
     */
    private boolean isOrphan() {
        return dbconfig == null;
    }

    /**
     * Synchronize the collection data with the underlying database.
     * @return Returns <code>True</code> when the synchronization is successful.
     */
    private synchronized boolean sync()
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
        ResultSet resultSet = stat.executeQuery(sqlQuery.build());
        conn.commit();
        int tupleId = 1;

        // fill the schema
        ResultSetMetaData metaData = resultSet.getMetaData();
        int count = metaData.getColumnCount();
        List<Column> columns = new ArrayList<Column>(count);
        for (int i = 1; i <= count; i ++) {
            String attributeName = metaData.getColumnName(i);
            columns.add(new Column(tableName, attributeName));
        }

        schema  = new Schema(tableName, columns);

        // fill the tuples
        while (resultSet.next()) {
            Object[] values = new Object[count];
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                if (attributeName.equalsIgnoreCase("tid")) {
                    tupleId = (int)resultSet.getObject(i);
                }
                values[i - 1] = resultSet.getObject(i);
            }

            tuples.add(new Tuple(tupleId, schema, values));
            tupleId ++;
        }
        stat.close();
        conn.close();
        return true;
    }

    //</editor-fold>

    //<editor-fold desc="Finalization methods">
    @Override
    protected void finalize() throws Throwable {
        if (isInternal) {
            Connection conn = null;
            try {
                conn = DBConnectionFactory.createConnection(dbconfig);
                Statement stat = conn.createStatement();
                stat.execute("DROP TABLE IF EXISTS " + tableName);
                conn.commit();
            } catch (Exception ex) {
                // ignore;
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
        }
        super.finalize();
    }
    //</editor-fold>
}
