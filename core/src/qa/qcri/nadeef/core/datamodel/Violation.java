/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Violation class.
 * TODO: considering using ORM libraries like JOOQ.
 */
public class Violation {
    private String ruleId;
    private Cell cell;
    private Object attributeValue;
    private int tupleId;

    //<editor-fold desc="Getter and Setters">
    public Cell getCell() {
        return cell;
    }

    public void setCell(Cell cell) {
        this.cell = cell;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public Object getAttributeValue() {
        return attributeValue;
    }

    public void setAttributeValue(Object attributeValue) {
        this.attributeValue = attributeValue;
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

        Violation violation = (Violation)obj;

        if (
            violation.getRuleId().equals(ruleId) &&
            violation.getCell().equals(cell) &&
            violation.getTupleId() == tupleId
        ) {
            return true;
        }
        return false;
    }
}
