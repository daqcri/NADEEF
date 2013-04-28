/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;

/**
 * FixExpression represents a candidate fix in the <code>Fix</code> table.
 */
public class FixExpression {

    //<editor-fold desc="Private fields">
    private Operation operation;
    private Cell left;
    private Cell right;
    private String rightValue;
    //</editor-fold>

    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     * @param left left operator.
     * @param right right operator.
     * @param operation operation value.
     */
    public FixExpression(Cell left, Cell right, Operation operation) {
        Preconditions.checkNotNull(left);
        Preconditions.checkNotNull(right);
        Preconditions.checkNotNull(operation);
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    /**
     * Constructor.
     * @param left left cell.
     * @param right right constant value in string.
     * @param operation operation value.
     */
    public FixExpression(Cell left, String right, Operation operation) {
        this.left = Preconditions.checkNotNull(left);
        this.rightValue = Preconditions.checkNotNull(right);
        this.operation = Preconditions.checkNotNull(operation);
    }

    //</editor-fold>

    //<editor-fold desc="Getters">

    /**
     * Gets the operation.
     * @return operation.
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Gets the left cell.
     * @return left cell.
     */
    public Cell getLeft() {
        return left;
    }

    /**
     * Gets the right value
     * @return
     */
    public Cell getRight() {
        return right;
    }

    /**
     * Returns <code>True</code> when the right cell is a constant value.
     * @return <code>True</code> when the right cell is a constant value.
     */
    public boolean isConstantAssign() {
        return this.right == null;
    }
    //</editor-fold>
}
