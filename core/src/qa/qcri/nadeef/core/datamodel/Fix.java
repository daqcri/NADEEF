/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Fix represents a suggestion of changing a cell to a value. This is the result from repairing
 * violations.
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
        private Optional<Operation> operation;

        public Builder() {
            operation = Optional.absent();
        }

        public Builder(Violation violation) {
            this();
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

        public Builder op(Operation operation) {
            this.operation = Optional.of(operation);
            return this;
        }

        public Fix build() {
            Fix fix;
            if (operation.isPresent()) {
                fix = new Fix(vid, left, right, operation.get());
                operation = Optional.absent();
            } else {
                if (rightValue != null) {
                    fix = new Fix(vid, left, rightValue, Operation.CEQ);
                } else {
                    fix = new Fix(vid, left, right, Operation.EQ);
                }
            }
            return fix;
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
    protected Fix(int vid, Cell left, Cell right, Operation operation) {
        this(vid, left, operation);
        this.right = Preconditions.checkNotNull(right);
    }

    /**
     * Constructor.
     * @param left left cell.
     * @param right right constant value in string.
     * @param operation operation value.
     */
    protected Fix(int vid, Cell left, String right, Operation operation) {
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
     * Gets the right value as a cell.
     * @return right value as a cell.
     */
    public Cell getRight() {
        return right;
    }

    /**
     * Gets the right value in string.
     * @return value in string.
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
