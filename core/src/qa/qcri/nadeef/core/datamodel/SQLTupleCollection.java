/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Tuple collection class.
 */
public class SQLTupleCollection extends TupleCollection {
    private DBConfig dbconfig;
    private String tableName;
    private SqlQueryBuilder sqlQuery;
    private ArrayList<Tuple> tuples;
    private long updateTimestamp = -1;
    private long changeTimestamp = System.currentTimeMillis();

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
     * It will call <code>syncData</code> if the collection is not yet existed.
     *
     * @return size of the collection.
     */
    @Override
    public int size() {
        syncDataIfNeeded();
        return tuples.size();
    }

    /**
     * Gets the schema of the TupleCollection.
     * @return the schema.
     */
    @Override
    public Schema getSchema() {
        syncSchemaIfNeeded();
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
        syncDataIfNeeded();
        return tuples.get(i);
    }

    /**
     * Projects the TupleCollection by column name.
     * @param columnName column name.
     * @return tuple collection itself.
     */
    @Override
    public TupleCollection project(String columnName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(columnName));
        sqlQuery.addSelect(columnName);
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    /**
     * Projects the <code>TupleCollection</code> by column.
     * @param column Column.
     * @return tuple collection itself.
     */
    @Override
    public TupleCollection project(Column column) {
        Preconditions.checkNotNull(column);
        sqlQuery.addSelect(column.getAttributeName());
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    @Override
    public TupleCollection project(Collection<Column> columns) {
        Preconditions.checkNotNull(columns);
        for (Column column : columns) {
            sqlQuery.addSelect(column.getAttributeName());
        }
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    @Override
    public TupleCollection orderBy(String columnName) {
        sqlQuery.addOrder(columnName);
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }

        return this;
    }

    @Override
    public TupleCollection orderBy(Column column) {
        sqlQuery.addOrder(column.getAttributeName());
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }

        return this;
    }

    @Override
    public TupleCollection orderBy(Collection<Column> columns) {
        for (Column column : columns) {
            sqlQuery.addOrder(column.getAttributeName());
        }
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    @Override
    public TupleCollection filter(SimpleExpression expression) {
        sqlQuery.addWhere(expression.toString());
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    @Override
    public TupleCollection filter(List<SimpleExpression> expressions) {
        for (SimpleExpression expression : expressions) {
            sqlQuery.addWhere(expression.toString());
        }
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    @Override
    public Collection<TupleCollection> groupOn(Collection<Column> columns) {
        List result = Lists.newArrayList(this);
        for (Column column : columns) {
            List<TupleCollection> tmp = Lists.newArrayList();
            for (Object collection : result) {
                tmp.addAll(((SQLTupleCollection) collection).groupOn(column));
            }
            result = tmp;
        }
        return result;
    }

    @Override
    public Collection<TupleCollection> groupOn(Column column) {
        Collection<TupleCollection> result = null;
        if (isOrphan()) {
            result = super.groupOn(column);
        } else {
            result = Lists.newArrayList();
            Connection conn = null;
            try {
                String sql =
                    "SELECT DISTINCT(" + column.getAttributeName() + ") FROM " + tableName;
                conn = DBConnectionFactory.getSourceConnection();
                Statement stat = conn.createStatement();
                ResultSet distinctResult = stat.executeQuery(sql);

                while (distinctResult.next()) {
                    Object value = distinctResult.getObject(1);
                    String stringValue = value.toString();
                    if (value instanceof String) {
                        stringValue = '\'' + value.toString() + '\'';
                    }
                    SimpleExpression columnFilter =
                        SimpleExpression.newEqual(column, stringValue);

                    SQLTupleCollection newTupleCollection =
                        new SQLTupleCollection(tableName, dbconfig);
                    newTupleCollection.sqlQuery = new SqlQueryBuilder(sqlQuery);
                    newTupleCollection.sqlQuery.addWhere(columnFilter.toString());
                    result.add(newTupleCollection);
                }
                conn.commit();
            } catch (Exception ex) {
                tracer.err(ex.getMessage(), ex);
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

    /**
     * Calculates the hash code of the <code>TupleCollection</code>.
     * @return hash code.
     */
    @Override
    public int hashCode() {
        return dbconfig.hashCode() * tableName.hashCode();
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
     * Synchronize the data schema with underneath database.
     */
    private synchronized void syncSchema() {
        if (isOrphan()) {
            tracer.info("Orphan SQLTupleCollection cannot be synced.");
            return;
        }

        Connection conn = null;
        try {
            SqlQueryBuilder builder = new SqlQueryBuilder(sqlQuery);
            builder.setLimit(1);
            conn = DBConnectionFactory.getSourceConnection();
            String sql = builder.build();
            tracer.verbose(sql);
            ResultSet resultSet = conn.createStatement().executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            List<Column> columns = new ArrayList<Column>();
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                columns.add(new Column(tableName, attributeName));
            }

            schema = new Schema(tableName, columns);
        } catch (Exception ex) {
            tracer.err("Cannot get valid schema.", ex);
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

    /**
     * Synchronize the collection data with the underlying database.
     * @return Returns <code>True</code> when the synchronization is successful.
     */
    private synchronized boolean syncData() {
        if (isOrphan()) {
            tracer.info("TupleCollection is an orphan, syncData failed.");
            return false;
        }

        Stopwatch stopwatch = new Stopwatch().start();
        Connection conn = null;
        try {
            tuples = Lists.newArrayList();
            conn = DBConnectionFactory.getSourceConnection();
            Statement stat = conn.createStatement();
            String sql = sqlQuery.build();
            tracer.verbose(sql);
            ResultSet resultSet = stat.executeQuery(sql);
            conn.commit();
            int tupleId = -1;

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
            }
            stat.close();
            conn.close();
        } catch (Exception ex) {
            tracer.err("Synchronization failed.", ex);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

        Tracer.addStatEntry(Tracer.StatType.DBLoadTime, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.stop();
        return true;
    }

    /**
     * Synchronize the schema and data if needed.
     */
    private synchronized void syncSchemaIfNeeded() {
        if (updateTimestamp < changeTimestamp) {
            syncSchema();
            updateTimestamp = changeTimestamp;
        }
    }

    private synchronized void syncDataIfNeeded() {
        if (updateTimestamp < changeTimestamp) {
            syncData();
            updateTimestamp = changeTimestamp;
        }
    }
    //</editor-fold>

    //<editor-fold desc="Finalization methods">
    public void recycle() {
        if (isInternal) {
            Connection conn = null;
            try {
                conn = DBConnectionFactory.getSourceConnection();
                Statement stat = conn.createStatement();
                stat.execute("DROP VIEW IF EXISTS " + tableName);
                conn.commit();
            } catch (Exception ex) {
                tracer.err("Exception from the finalizer. ", ex);
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
        tuples.clear();
        tuples = null;
        tableName = null;
        dbconfig = null;
        updateTimestamp = Long.MAX_VALUE;
    }

    //</editor-fold>
}
