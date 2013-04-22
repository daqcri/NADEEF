/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;

/**
 * Schema class provides a mapping between column and value for a table.
 */
public class Schema {
    private String tableName;
    private HashMap<Column, Integer> mapping;

    /**
     * Constructor.
     * @param tableName table name.
     * @param columns column array.
     */
    public Schema(String tableName, Column[] columns) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkArgument(columns != null && columns.length > 0);
        this.tableName = tableName;
        mapping = new HashMap(columns.length);
        for (int i = 0; i < columns.length; i ++) {
            mapping.put(columns[i], i);
        }
    }

    /**
     * The size of the schema.
      * @return size.
     */
    public int size() {
        return mapping.size();
    }

    /**
     * Returns <code>True</code> when the map contains the column.
     * @param column column.
     * @return <code>True</code> when the map contains the column.
     */
    public boolean hasColumn(Column column) {
        return mapping.containsKey(column);
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
    // TODO: do a caching
    public ImmutableSet<Column> getColumns() {
        return ImmutableSet.copyOf(mapping.keySet());
    }

    /**
     * Gets the index from the column.
     * @param column
     * @return Get the index from column.
     */
    public Integer get(Column column) {
        return mapping.get(column);
    }
}
