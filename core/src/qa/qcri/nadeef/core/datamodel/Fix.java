/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;

/**
 * Fix represents a candidate fix in the <code>Fix</code> table.
 */
public class Fix {

    //<editor-fold desc="Private fields">
    private Operation operation;
    private Cell left;
    private Cell right;
    private String rightValue;

    //</editor-fold>

    public static class Builder {
        private Cell left;
        private Cell right;
        private String rightValue;

        public Builder() {}

        public Builder left(Cell left) {
            this.left = Preconditions.checkNotNull(left);
            return this;
        }

        public Builder right(Cell right) {
            this.right = Preconditions.checkNotNull(right);
            this.rightValue = null;
            return this;
        }

        public Builder right(String right) {
            this.rightValue = Preconditions.checkNotNull(right);
            this.right = null;
            return this;
        }

        public Fix build() {
            if (rightValue != null) {
                return new Fix(left, rightValue, Operation.EQ);
            }
            return new Fix(left, right, Operation.EQ);
        }
    }
    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     * @param left left operator.
     * @param right right operator.
     * @param operation operation value.
     */
    public Fix(Cell left, Cell right, Operation operation) {
        this.left = Preconditions.checkNotNull(left);
        this.right = Preconditions.checkNotNull(right);
        this.operation = Preconditions.checkNotNull(operation);
    }

    /**
     * Constructor.
     * @param left left cell.
     * @param right right constant value in string.
     * @param operation operation value.
     */
    public Fix(Cell left, String right, Operation operation) {
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
     * Gets the right value
     * @return
     */
    public String getRightValue() {
        return rightValue;
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
