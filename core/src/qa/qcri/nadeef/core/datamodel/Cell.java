/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * A Cell represents a basic unit in a data table. A Cell
 * contains a <code>Column</code> and a Value with an unique TupleId.
 */
public class Cell {
    //<editor-fold desc="Private fields">
    private int tid;
    private Column column;
    private Object value;
    //</editor-fold>

    /**
     * Cell builder class.
     */
    public static class Builder {
        private int tupleId;
        private Column column;
        private Object value;

        public Builder() {}

        public Builder tid(int tid) {
            this.tupleId = tid;
            return this;
        }

        public Builder column(Column column) {
            this.column = column;
            return this;
        }

        public Builder column(String column) {
            this.column = new Column(column);
            return this;
        }

        public Builder value(Object value) {
            this.value = value;
            return this;
        }

        public Cell build() {
            return new Cell(column, tupleId, value);
        }
    }

    //<editor-fold desc="Constructors">
    /**
     * Constructor.
     * @param column Column.
     * @param tid TupleId.
     * @param value Value.
     */
    public Cell(Column column, int tid, Object value) {
        this.column = column;
        this.value = value;
        this.tid = tid;
    }
    //</editor-fold>

    //<editor-fold desc="Getter and Setters">

    /**
     * Gets the column.
     * @return column.
     */
    public Column getColumn() {
        return column;
    }

    /**
     * Gets the column value.
     * @return column value.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Gets Tuple Id.
     * @return tuple id.
     */
    public int getTupleId() {
        return tid;
    }

    /**
     * Returns <code>True</code> when the Cell has the same column name as the input.
     * @param columnName column name.
     * @return <code>True</code> when the Cell has the same column name as the input.
     */
    public boolean hasColumnName(String columnName) {
        return column.getColumnName().equalsIgnoreCase(columnName);
    }

    //</editor-fold>

    //<editor-fold desc="Override Equals and Hash code">

    /**
     * Returns <code>True</code> when the given object is as the same as the input.
     * @param obj input object.
     * @return <code>True</code> when the given object is as the same as the input.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Cell)) {
            return false;
        }

        Cell row = (Cell)obj;

        return row.column.equals(column) && row.tid == tid;
    }

    /**
     * Returns the hash code of the column.
     * @return the hash code of the column.
     */
    @Override
    public int hashCode() {
        return column.hashCode() * tid;
    }
    //</editor-fold>
}
