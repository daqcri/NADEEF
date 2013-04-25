/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import qa.qcri.nadeef.core.exception.InvalidSchemaException;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.SqlQueryBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema class provides a mapping between column and value for a table.
 */
public class Schema {
    private String tableName;
    private ImmutableMap<Column, Integer> map;
    private ImmutableSet<Column> columnSet;

    /**
     * Constructor.
     * @param tableName table name.
     * @param columns column array.
     */
    public Schema(String tableName, List<Column> columns) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkArgument(columns != null && columns.size() > 0);
        this.tableName = tableName;
        Map mapping = new HashMap(columns.size());

        for (int i = 0; i < columns.size(); i ++) {
            mapping.put(columns.get(i), i);
        }
        columnSet = ImmutableSet.copyOf(mapping.keySet());
        map = ImmutableMap.copyOf(mapping);
    }

    /**
     * The size of the schema.
      * @return size.
     */
    public int size() {
        return map.size();
    }

    /**
     * Returns <code>True</code> when the map contains the column.
     * @param column column.
     * @return <code>True</code> when the map contains the column.
     */
    public boolean hasColumn(Column column) {
        return map.containsKey(column);
    }

    /**
     * Gets the table name.
     * @return table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the column collection.
     * @return column collection.
     */
    public ImmutableSet<Column> getColumns() {
        return columnSet;
    }

    /**
     * Gets the index from the column.
     * @param column
     * @return Get the index from column.
     */
    public Integer get(Column column) {
        return map.get(column);
    }
}
