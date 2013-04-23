/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Violation row.
 */
public class Cell {
    private int tupleId;
    private Column column;
    private Object value;

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
    //</editor-fold>

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Violation)) {
            return false;
        }

        Cell row = (Cell)obj;

        if (row.column.equals(column) && row.tupleId == tupleId) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return column.hashCode() * tupleId;
    }
}
