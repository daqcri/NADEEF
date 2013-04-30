/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Operation enumeration.
 */
public enum Operation {
    EQ(0),
    LT(1),
    GT(2),
    NEQ(3),
    LTE(4),
    GTE(5),
    CEQ(6);

    private final int value;
    private Operation(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
