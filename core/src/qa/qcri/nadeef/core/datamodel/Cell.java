/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * A Cell contains a Column and a Value given a TupleId.
 */
public class Cell {
    //<editor-fold desc="Private fields">
    private int tupleId;
    private Column column;
    private Object value;
    //</editor-fold>

    /**
     * Builder class.
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
     * @param tupleId TupleId.
     * @param value Value.
     */
    public Cell(Column column, int tupleId, Object value) {
        this.column = column;
        this.value = value;
        this.tupleId = tupleId;
    }
    //</editor-fold>

    //<editor-fold desc="Getter and Setters">
    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public Object getAttributeValue() {
        return value;
    }

    public void setAttributeValue(Object value) {
        this.value = value;
    }

    public int getTupleId() {
        return tupleId;
    }

    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    public boolean containsAttribute(String attribute) {
        return column.getAttributeName().equalsIgnoreCase(attribute);
    }

    //</editor-fold>

    //<editor-fold desc="Override Equals and Hash code">
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Cell)) {
            return false;
        }

        Cell row = (Cell)obj;

        return row.column.equals(column) && row.tupleId == tupleId;
    }

    @Override
    public int hashCode() {
        return column.hashCode() * tupleId;
    }
    //</editor-fold>
}
