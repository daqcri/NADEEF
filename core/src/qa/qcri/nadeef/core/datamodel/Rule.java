/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract base class for a rule.
 */
public abstract class Rule<T> extends AbstractRule<T> {
    protected RuleHintCollection hints;

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected Rule(String id, List<String> tableNames) {
        super(id, tableNames);
    }

    /**
     * Default group operation.
     * @param tupleCollection input tuple
     * @return a group of tuple collection.
     */
    @Override
    public Collection<TupleCollection> group(TupleCollection tupleCollection) {
        Collection<TupleCollection> result = new LinkedList<TupleCollection>();
        return result;
    }

    /**
     * Default filter operation.
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public TupleCollection filter(TupleCollection tupleCollection) {
        return tupleCollection;
    }

    /**
     * Getter of hint.
     * @return opitmization hints.
     */
    public RuleHintCollection getHints() {
        return this.hints;
    }
}