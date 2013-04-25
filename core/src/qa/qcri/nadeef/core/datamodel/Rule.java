/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Collection;
import java.util.List;

/**
 * Abstract rule.
 */
public abstract class Rule<TDetect, TIterator> {
    protected String id;
    protected List<String> tableNames;

    /**
     * Default constructor.
     */
    protected Rule() {}

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    public Rule(String id, List<String> tableNames) {
        initialize(id, tableNames);
    }

    /**
     * Initialize a rule.
     * @param id Rule id.
     * @param tableNames table names.
     */
    void initialize(String id, List<String> tableNames) {
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(id) && tableNames != null && tableNames.size() > 0
        );

        this.id = id;
        this.tableNames = tableNames;
    }

    /**
     * Gets of rule Id.
     */
    public String getId() {
        return id;
    }

    /**
     * Detect rule with one tuple.
     * @param tuples input tuple.
     * @return Violation set.
     */
    public abstract Collection<Violation> detect(TDetect tuples);

    // public abstract Collection<Fix> repair(Violation violation);
    /**
     * Default group operation.
     * @param tupleCollection input tuple
     * @return a group of tuple collection.
     */
    public abstract TIterator group(Collection<TupleCollection> tupleCollection);

    /**
     * Default scope operation.
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    public abstract Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollection);

    /**
     * Returns <code>True</code> when the rule implements one tuple input.
     * @return <code>True</code> when the rule implements one tuple inputs.
     */
    public boolean supportOneInput() {
        return this instanceof SingleTupleRule;
    }

    /**
     * Returns <code>True</code> when the rule implements two tuple inputs.
     * @return <code>True</code> when the rule implements two tuple inputs.
     */
    public boolean supportTwoInputs() {
        return this instanceof PairTupleRule;
    }

    /**
     * Returns <code>True</code> when the rule implements multiple tuple inputs.
     * @return <code>True</code> when the rule implements multiple tuple inputs.
     */
    public boolean supportManyInputs() {
        return this instanceof SingleTupleRule;
    }

    /**
     * Returns <code>True</code> when the rule has two tables supported.
     * @return <code>True</code> when the rule has two tables supported.
     */
    public boolean supportTwoTables() {
        return tableNames.size() == 2;
    }

    /**
     * Gets the used table names in the rule.
     * @return A list of table names.
     */
    public List<String> getTableNames() {
        return tableNames;
    }
}
