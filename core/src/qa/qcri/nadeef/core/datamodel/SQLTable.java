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

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.sql.NadeefSQLDialectManagerBase;
import qa.qcri.nadeef.core.util.sql.SQLDialectManagerFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SQLTable represents a {@link Table} which resides in a database.
 */
public class SQLTable extends Table {
    private DBConnectionFactory connectionFactory;
    private NadeefSQLDialectManagerBase dialectManager;
    private String tableName;
    private SqlQueryBuilder sqlQuery;
    private List<Tuple> tuples;
    private long updateTimestamp = -1;
    private long changeTimestamp = System.currentTimeMillis();

    private static Tracer tracer = Tracer.getTracer(SQLTable.class);
    private static Object indexLock = new Object();

    //<editor-fold desc="Constructor">
    /**
     * Constructor with database connection.
     * @param tableName tuple collection table name.
     * @param connectionFactory database connection pool.
     */
    public SQLTable(String tableName, DBConnectionFactory connectionFactory) {
        super(tableName);
        this.connectionFactory = connectionFactory;
        this.dialectManager =
            SQLDialectManagerFactory.getDialectManagerInstance(
                    connectionFactory.getSourceDBConfig().getDialect()
            );
        this.tableName = tableName;
        this.sqlQuery = new SqlQueryBuilder();
        this.sqlQuery.addFrom(tableName);
    }

    //</editor-fold>

    //<editor-fold desc="Table Interface">
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
     * Gets the schema of the Table.
     * @return the schema.
     */
    @Override
    public Schema getSchema() {
        syncSchemaIfNeeded();
        return schema;
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
     * {@inheritDoc}
     */
    @Override
    public Table project(List<Column> columns) {
        Preconditions.checkNotNull(columns);
        for (Column column : columns) {
            sqlQuery.addSelect(column.getColumnName());
        }
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table orderBy(List<Column> columns) {
        for (Column column : columns) {
            sqlQuery.addOrder(column.getColumnName());
        }
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table filter(List<SimpleExpression> expressions) {
        for (SimpleExpression expression : expressions) {
            sqlQuery.addWhere(expression.toString());
        }
        synchronized (this) {
            changeTimestamp = System.currentTimeMillis();
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public Collection<Table> groupOn(List<Column> columns) {
        List result = Lists.newArrayList(this);
        for (Column column : columns) {
            List<Table> tmp = Lists.newArrayList();
            for (Object collection : result) {
                tmp.addAll(((SQLTable) collection).groupOn(column));
            }
            result = tmp;
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Table> groupOn(Column column) {
        Collection<Table> result = Lists.newArrayList();
        Connection conn = null;
        Statement stat = null;
        ResultSet distinctResult = null;
        String indexName = null;

        try {
            conn = connectionFactory.getSourceConnection();
            // create index ad-hoc.
            stat = conn.createStatement();
            stat.setFetchSize(4096);

            synchronized (indexLock) {
                indexName =
                    "IDX_" + tableName + "_" + column.getColumnName() + "_"
                    + System.currentTimeMillis();

                // create the index.
                String indexSQL =
                    "CREATE INDEX " + indexName + " ON " +
                    tableName + " (" + column.getColumnName() + ")";
                stat.executeUpdate(indexSQL);
                conn.commit();
            }

            String sql =
                "SELECT DISTINCT(" + column.getColumnName() + ") FROM " + tableName;
            distinctResult = stat.executeQuery(sql);

            while (distinctResult.next()) {
                Object value = distinctResult.getObject(1);
                String stringValue = value.toString();
                if (value instanceof String) {
                    stringValue = '\'' + value.toString() + '\'';
                }
                SimpleExpression columnFilter =
                    SimpleExpression.newEqual(column, stringValue);

                SQLTable newTable =
                    new SQLTable(tableName, connectionFactory);
                newTable.sqlQuery = new SqlQueryBuilder(sqlQuery);
                newTable.sqlQuery.addWhere(columnFilter.toString());
                result.add(newTable);
            }
        } catch (Exception ex) {
            tracer.err(ex.getMessage(), ex);
            // as a backup plan we try to use in-memory solution.
            result = super.groupOn(column);
        } finally {
            if (distinctResult != null) {
                try {
                    distinctResult.close();
                } catch (Exception ex) {}
            }

            if (indexName != null) {
                try {
                    stat.executeUpdate("DROP INDEX " + indexName);
                } catch (Exception ex) {
                    // ignore;
                }
            }

            if (stat != null) {
                try {
                    stat.close();
                } catch (Exception ex) {};
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
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

        if (collection == null || !(collection instanceof SQLTable)) {
            return false;
        }

        SQLTable obj = (SQLTable)collection;
        DBConfig dbConfig1 = connectionFactory.getSourceDBConfig();
        DBConfig dbConfig2 = obj.connectionFactory.getSourceDBConfig();
        if (dbConfig1.equals(dbConfig2) && tableName.equals(obj.tableName)) {
            return true;
        }

        return false;
    }

    /**
     * Calculates the hash code of the <code>Table</code>.
     * @return hash code.
     */
    @Override
    public int hashCode() {
        DBConfig dbconfig = connectionFactory.getSourceDBConfig();
        return dbconfig.hashCode() * tableName.hashCode();
    }

    //</editor-fold>

    //<editor-fold desc="Private members">

    /**
     * Synchronize the data schema with underneath database.
     */
    private void syncSchema() {
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        try {
            SqlQueryBuilder builder = new SqlQueryBuilder(sqlQuery);
            builder.setLimit(1);
            String sql = builder.build(dialectManager);
            tracer.verbose(sql);

            conn = connectionFactory.getSourceConnection();
            stat = conn.createStatement();

            resultSet = stat.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            Column[] columns = new Column[count];
            int[] types = new int[count];
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                columns[i - 1] = new Column(tableName, attributeName);
                types[i - 1] = metaData.getColumnType(i);
            }

            schema = new Schema(tableName, columns, types);
        } catch (Exception ex) {
            tracer.err("Cannot get valid schema.", ex);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception ex) {}
            }

            if (stat != null) {
                try {
                    stat.close();
                } catch (Exception ex) {};
            }

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
    private boolean syncData() {
        Stopwatch stopwatch = new Stopwatch().start();
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        try {
            // prepare for the SQL
            String sql = sqlQuery.build(dialectManager);
            tracer.verbose(sql);

            // get the connection and run the SQL
            conn = connectionFactory.getSourceConnection();
            stat = conn.createStatement();
            stat.setFetchSize(4096);
            resultSet = stat.executeQuery(sql);

            // fill the schema
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            int tidIndex = 0;
            Column[] columns = new Column[count];
            int[] types = new int[count];
            for (int i = 1; i <= count; i ++) {
                String columnName = metaData.getColumnName(i);
                columns[i - 1] = new Column(tableName, columnName);
                types[i - 1] = metaData.getColumnType(i);
                if (tidIndex == 0 && columnName.equalsIgnoreCase("tid")) {
                    tidIndex = i;
                }
            }
            schema  = new Schema(tableName, columns, types);

            // fill the tuples
            tuples = Lists.newArrayList();
            int tupleId = -1;
            while (resultSet.next()) {
                List<byte[]> values = Lists.newArrayList();
                if (tidIndex != 0) {
                    tupleId = resultSet.getInt(tidIndex);
                } else {
                    tracer.info("Table does not have an TID column, use -1 as default.");
                    tupleId = -1;
                }

                for (int i = 1; i <= count; i ++) {
                    Object object = resultSet.getObject(i);
                    values.add(serialize(object));
                }

                tuples.add(new Tuple(tupleId, schema, values));
            }
        } catch (Exception ex) {
            tracer.err("Synchronization failed.", ex);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception ex) {}
            }

            if (stat != null) {
                try {
                    stat.close();
                } catch (Exception ex) {};
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

        Tracer.addStatsEntry(Tracer.StatType.DBLoadTime, stopwatch.elapsed(TimeUnit.MILLISECONDS));
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

    /**
     * Serialize object to a bytes array.
     */
    // TODO: check for UTF-16 for i18 cases.
    private static byte[] serialize(Object obj) throws IOException {
        byte[] result = null;
        if (obj instanceof String) {
            result = ((String)obj).getBytes(Charset.forName("UTF-8"));
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            result = out.toByteArray();
        }

        return result;
    }

    //</editor-fold>

    //<editor-fold desc="Finalization methods">
    public void recycle() {
        tuples.clear();
        tuples = null;
        tableName = null;
        updateTimestamp = Long.MAX_VALUE;
        connectionFactory = null;
    }

    //</editor-fold>
}
