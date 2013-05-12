/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.Collection;
import java.util.List;

/**
 * Abstract base class for a rule providing the default behavior of scope and generator operation.
 */
public abstract class SingleTupleRule extends Rule<Tuple, Collection<TupleCollection>> {
    /**
     * Default constructor.
     */
    public SingleTupleRule() {}

    /**
     * Internal method to initialize a rule.
     * @param id Rule id.
     * @param tableNames Table names.
     */
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    /**
     * Default scope operation.
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollection) {
        return tupleCollection;
    }

    /**
     * Default generator operation.
     * @param tupleCollection input tuple
     * @return grouped tuple collection.
     */
    @Override
    public Collection<TupleCollection> generator(Collection<TupleCollection> tupleCollection) {
        return tupleCollection;
    }

}