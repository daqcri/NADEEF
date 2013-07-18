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

import java.util.Collection;
import java.util.List;

/**
 * SingleTupleRule rule is an abstract class for rule which has detection based on one tuple.
 */
public abstract class SingleTupleRule extends Rule<Tuple> {
    /**
     * Default constructor.
     */
    public SingleTupleRule() {}

    /**
     * Internal method to initialize a rule.
     * @param name Rule name.
     * @param tableNames Table names.
     */
    public void initialize(String name, List<String> tableNames) {
        super.initialize(name, tableNames);
    }

    /**
     * Default scope operation.
     * @param table input tables.
     * @return filtered table.
     */
    @Override
    public Collection<Table> horizontalScope(Collection<Table> table) {
        return table;
    }

    /**
     * Default scope operation.
     * @param table input table.
     * @return filtered table.
     */
    @Override
    public Collection<Table> verticalScope(Collection<Table> table) {
        return table;
    }

    /**
     * Block operator.
     *
     * Current we don't support blocking given multiple tables (co-group). So when a rule
     * is using more than 1 table the block operator is going to be ignored.
     * @param table a collection of tables.
     * @return a collection of blocked tables.
     */
    @Override
    public Collection<Table> block(Collection<Table> table) {
        return table;
    }

    /**
     * Default iterator operation.
     * @param tables input table collection.
     */
    @Override
    public void iterator(Collection<Table> tables, IteratorStream<Tuple> iteratorStream) {
        Table table = tables.iterator().next();
        for (int i = 0; i < table.size(); i ++) {
            iteratorStream.put(table.get(i));
        }
    }

}