/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
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
 * @author Si Yin <siyin@qf.org.qa>
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
     */
    // TODO: currently for numerical value we do a simple regex to check,
    // but it is not 100% correct since numerical value can also be used
    // as string in SQL.
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

    /**
     * Returns <code>True</code> when the tuple matches the given expression.
     * @param tuple tuple.
     * @return <code>True</code> when the tuple matches the given expression.
     */
    public boolean match(Tuple tuple) {
        Object value = tuple.get(left);
        if (value.equals(this.value)) {
            return true;
        }
        return false;
    }

    /**
     * Gets the {@link Operation}.
     * @return operation.
     */
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
