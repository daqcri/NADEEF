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
import java.util.List;

/**
 * Abstract rule.
 */
public abstract class AbstractRule<T> {
    protected String id;
    protected List<String> tableNames;
    protected Class inputType;

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    public AbstractRule(String id, List<String> tableNames) {
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(id) && tableNames != null && tableNames.size() > 0
        );

        this.id = id;
        this.tableNames = tableNames;

        // reflect on the input type;
        ParameterizedType parameterizedType =
                (ParameterizedType)getClass().getGenericSuperclass();

        Type[] types = parameterizedType.getActualTypeArguments();
        inputType = (Class)types[0];
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
    public abstract Collection<Violation> detect(T tuples);

    /**
     * Default group operation.
     * @param tupleCollection input tuple
     * @return a group of tuple collection.
     */
    public abstract Collection<TupleCollection> group(TupleCollection tupleCollection);

    /**
     * Default filter operation.
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    public abstract TupleCollection filter(TupleCollection tupleCollection);

    /**
     * Returns <code>True</code> when the rule implements one tuple input.
     * @return <code>True</code> when the rule implements one tuple inputs.
     */
    public boolean supportOneInput() {
        return inputType == Tuple.class;
    }

    /**
     * Returns <code>True</code> when the rule implements two tuple inputs.
     * @return <code>True</code> when the rule implements two tuple inputs.
     */
    public boolean supportTwoInputs() {
        return inputType == TuplePair.class;
    }

    /**
     * Returns <code>True</code> when the rule implements multiple tuple inputs.
     * @return <code>True</code> when the rule implements multiple tuple inputs.
     */
    public boolean supportManyInputs() {
        return inputType == TupleCollection.class;
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
