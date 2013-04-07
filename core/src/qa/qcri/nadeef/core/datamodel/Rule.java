/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.Connection;

/**
 * Rule input enumeration.
 */
enum RuleInputType {
    One,
    Two,
    Many
}

/**
 * Abstract base class for a rule.
 */
public abstract class Rule extends Primitive {
    protected boolean isSQLSupported;
    protected RuleInputType numberOfInput;

    /**
     * Detect rule with one tuple.
     * @param tuple input tuple.
     * @return Violation set.
     */
    public Violation detect(Tuple tuple) {
        throw new NotImplementedException();
    }

    /**
     * Detect rule with two tuples.
     * @param tuple1 tuple 1.
     * @param tuple2 tuple 2.
     * @return Violation set.
     */
    public Violation detect(Tuple tuple1, Tuple tuple2) {
        throw new NotImplementedException();
    }

    /**
     * Detect rule with multiple tuples.
     * @param tupleIterator tuple iterator.
     * @return Violation set.
     */
    public Violation detect(Iterable<Tuple> tupleIterator) {
        throw new NotImplementedException();
    }

    /**
     * Detect rule which runs in SQL.
     * @return Violation set.
     */
    public Violation detectInSQL(Connection conn) {
        throw new NotImplementedException();
    }

    /**
     * Whether this rule can be executed in SQL.
     */
    public boolean isSQLSupported() {
        return this.isSQLSupported;
    }

    /**
     * Number of inputs this rule requires.
     * @return number of input.
     */
    public RuleInputType getInputType() {
        return this.numberOfInput;
    }
}