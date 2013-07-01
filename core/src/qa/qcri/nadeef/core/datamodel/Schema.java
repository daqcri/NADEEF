/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Schema class provides a mapping between column and value for a table.
 */
public class Schema {
    private String tableName;
    private Column[] columns;

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
            Column[] result = new Column[columns.size()];
            columns.toArray(result);
            return new Schema(tableName, result);
        }
    }
    //</editor-fold>

    /**
     * Constructor.
     * @param tableName table name.
     * @param columns column array.
     */
    public Schema(String tableName, Column[] columns) {
        this.tableName = Preconditions.checkNotNull(tableName);
        Preconditions.checkArgument(columns != null && columns.length > 0);
        this.columns = columns;
    }

    /**
     * Copy consturctor.
     * @param schema input schema.
     */
    public Schema(Schema schema) {
        Preconditions.checkNotNull(schema);
        this.tableName = schema.tableName;
        this.columns = new Column[schema.columns.length];
        for (int i = 0; i < schema.columns.length; i++) {
            this.columns[i] = schema.columns[i];
        }
    }

    /**
     * The size of the schema.
      * @return size.
     */
    public int size() {
        return columns.length;
    }

    /**
     * Returns <code>True</code> when the map contains the column.
     * @param column column.
     * @return <code>True</code> when the map contains the column.
     */
    public boolean hasColumn(Column column) {
        for (Column column_ : columns) {
            if (column_.equals(column)) {
                return true;
            }
        }
        return false;
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
    public Column[] getColumns() {
        return columns;
    }

    /**
     * Gets the index from the column.
     * @param column input column.
     * @return Get the index from column.
     */
    public int get(Column column) {
        for (int i = 0; i < columns.length; i ++) {
            if (columns[i].equals(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Cannot find the column.");
    }

    /**
     * Returns the TID index of the schema. It returns absent when there is no TID column.
     * @return Returns the TID index of the schema. It returns absent when there is no TID column.
     */
    public Optional<Integer> getTidIndex() {
        for (int i = 0; i < columns.length; i ++) {
            if (columns[i].getColumnName().equalsIgnoreCase("tid")) {
                return Optional.of(i);
            }
        }
        return Optional.absent();
    }

    /**
     * Removes one column.
     * @param column column to be removed.
     */
    void remove(Column column) {
        int result = get(column);
        List<Column> tmp = Lists.newArrayList();
        for (int i = 0; i < columns.length; i ++) {
            if (i != result) {
                tmp.add(columns[i]);
            }
        }
        columns = new Column[tmp.size()];
        tmp.toArray(columns);
    }
}
