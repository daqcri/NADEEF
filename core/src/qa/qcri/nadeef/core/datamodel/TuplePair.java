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

import com.google.common.base.Preconditions;

/**
 * Tuple Pair represents a pair of tuples.
 */
public class TuplePair {
    private Tuple left;
    private Tuple right;

    /**
     * Constructor.
     * @param left left tuple.
     * @param right right tuple.
     */
    public TuplePair(Tuple left, Tuple right) {
        Preconditions.checkArgument(
            left != null && right != null, "Pair cannot have null values."
        );
        this.left = left;
        this.right = right;
    }

    /**
     * Private constructor.
     */
    TuplePair() {}

    //<editor-fold desc="Getters">

    /**
     * Get the left tuple.
     * @return the left tuple.
     */
    public Tuple getLeft() {
        return left;
    }

    /**
     * Gets the right tuple.
     * @return the right tuple.
     */
    public Tuple getRight() {
        return right;
    }

    //</editor-fold>
}
