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
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;
import qa.qcri.nadeef.tools.CommonTools;

import java.util.Collections;
import java.util.Map;

/**
 * Expression class describes a simple expression used in the scope.
 * An expression contains a left operator and a assigned value.
 */
public class SimpleExpression {

    /**
     * Initialize BiMap for operation and corresponding SQL strings.
     */
    private static final BiMap<Operation, String> operationMap;
    static {
        Map<Operation, String> realMap = Maps.newHashMap();
        realMap.put(Operation.EQ, "=");
        realMap.put(Operation.GT, ">");
        realMap.put(Operation.LT, "<");
        realMap.put(Operation.NEQ, "!=");
        realMap.put(Operation.GTE, ">=");
        realMap.put(Operation.LTE, "<=");
        realMap.put(Operation.CEQ, "<-");
        operationMap = ImmutableBiMap.copyOf(Collections.unmodifiableMap(realMap));
    }

    private Operation operation;
    private Column left;
    private String value;

    /**
     * Constructor.
     * @param operation operation.
     * @param left left column.
     * @param value right value.
     */
    public SimpleExpression(
        Operation operation,
        Column left,
        String value
    ) {
        Preconditions.checkNotNull(operation);
        Preconditions.checkNotNull(left);
        Preconditions.checkNotNull(value);
        this.operation = operation;
        this.left = left;
        this.value = value;
    }

    /**
     * Creates a <code>SimpleExpression</code> from string.
     * @param input string.
     * @param defaultTableName default table name.
     * @return <code>SimpleExpression</code> based on the input.
     */
    public static SimpleExpression fromString(String input, String defaultTableName) {
        String[] splits = input.split("\\s");
        if (splits.length != 3) {
            throw new IllegalArgumentException("Invalid expression string " + input);
        }

        Column column;
        if (!CommonTools.isValidColumnName(splits[0])) {
            column = new Column(defaultTableName, splits[0]);
        } else {
            column = new Column(splits[0]);
        }

        Operation operation = operationMap.inverse().get(splits[1]);
        return new SimpleExpression(operation, column, splits[2]);
    }

    /**
     * Create a <code>SimpleExpression</code> with <code>Equal</code> operation.
     * @param column column name.
     * @param value value of the column.
     * @return An Equal expression.
     */
    public static SimpleExpression newEqual(Column column, String value) {
        return new SimpleExpression(Operation.EQ, column, value);
    }

    /**
     * Returns the expression in SQL String.
     * @return SQL String.
     * TODO: currently for numerical value we do a simple regex to check,
     * but it is not 100% correct since numerical value can also be used
     * as string in SQL.
     */
    public String toString() {
        StringBuilder builder = new StringBuilder(left.getFullColumnName());
        builder.append(operationMap.get(operation));
        if (value.matches("^[0-9]+$") || value.contains("'") || value.contains("\"")) {
            builder.append(value);
        } else {
            builder.append("'");
            builder.append(value);
            builder.append("'");
        }
        return builder.toString();
    }

    public Operation getOperation() {
        return operation;
    }

    /**
     * Gets the left operator column.
     * @return left operator column.
     */
    public Column getLeft() {
        return left;
    }

    /**
     * Gets the value of the operator.
     * @return the value of the operator.
     */
    public String getValue() {
        return value;
    }
}
