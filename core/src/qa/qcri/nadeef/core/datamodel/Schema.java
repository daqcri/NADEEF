/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Schema class provides a mapping between column and value for a table.
 */
public class Schema {
    private String tableName;
    private ImmutableMap<Column, Integer> map;
    private ImmutableSet<Column> columnSet;

    //<editor-fold desc="Builder">
    /**
     * Builder class.
     */
    public static class Builder {
        private String tableName;
        List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public Builder table(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder column(Column column) {
            columns.add(column);
            return this;
        }

        public Builder column(String columnName) {
            columns.add(new Column(tableName, columnName));
            return this;
        }

        public Schema build() {
            return new Schema(tableName, columns);
        }

        public Builder reset() {
            columns.clear();
            tableName = null;
            return this;
        }
    }
    //</editor-fold>

    /**
     * Constructor.
     * @param tableName table name.
     * @param columns column array.
     */
    public Schema(String tableName, List<Column> columns) {
        this.tableName = Preconditions.checkNotNull(tableName);
        Preconditions.checkArgument(columns != null && columns.size() > 0);
        Map<Column, Integer> mapping = Maps.newHashMap();

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

    /**
     * Returns the TID index of the schema. It returns absent when there is no TID column.
     * @return Returns the TID index of the schema. It returns absent when there is no TID column.
     */
    public Optional<Integer> getTidIndex() {
        Column tidColumn = new Column(tableName, "tid");
        if (columnSet.contains(tidColumn)) {
            return Optional.of(get(tidColumn));
        }
        return Optional.absent();
    }
}
