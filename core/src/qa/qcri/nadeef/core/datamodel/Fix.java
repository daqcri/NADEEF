/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Fix represents a candidate fix in the <code>Fix</code> table.
 */
public class Fix {

    //<editor-fold desc="Private fields">
    private Operation operation;
    private Cell left;
    private Cell right;
    private String rightValue;
    private int vid;

    //</editor-fold>

    /**
     * Builder class.
     */
    public static class Builder {
        private Cell left;
        private Cell right;
        private String rightValue;
        private int vid;

        public Builder() {}
        public Builder(Violation violation) {
            this.vid = violation.getVid();
        }

        public Builder vid(int vid) {
            this.vid = vid;
            return this;
        }

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
                return new Fix(vid, left, rightValue, Operation.CEQ);
            }
            return new Fix(vid, left, right, Operation.EQ);
        }
    }
    //<editor-fold desc="Constructor">
    protected Fix(int vid, Cell left, Operation operation) {
        this.left = Preconditions.checkNotNull(left);
        this.operation = Preconditions.checkNotNull(operation);
        this.vid = vid;
    }

    /**
     * Constructor.
     * @param left left operator.
     * @param right right operator.
     * @param operation operation value.
     */
    public Fix(int vid, Cell left, Cell right, Operation operation) {
        this(vid, left, operation);
        this.right = Preconditions.checkNotNull(right);
    }

    /**
     * Constructor.
     * @param left left cell.
     * @param right right constant value in string.
     * @param operation operation value.
     */
    public Fix(int vid, Cell left, String right, Operation operation) {
        this(vid, left, operation);
        this.rightValue = Preconditions.checkNotNull(right);
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
     * Gets of vid.
     * @return vid.
     */
    public int getVid() {
        return vid;
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
