/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import qa.qcri.nadeef.tools.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.util.*;

/**
 * Abstract base class for a rule providing the default behavior of filter and group operation.
 */
// TODO: adds generic verification to limit the T type.
public abstract class Rule<T> extends AbstractRule<T> {
    protected Set<Column> verticalFilter;
    protected Set<SimpleExpression> horizontalFilter;
    protected Column groupColumn;
    protected Set<String> groupSelector;
    protected String iteratorClass;
    protected Tracer tracer = Tracer.getTracer(getClass());

    /**
     * Default constructor.
     */
    public Rule() {}

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected Rule(String id, List<String> tableNames) {
        initialize(id, tableNames);
    }

    /**
     * Internal method to initialize a rule.
     * @param id Rule id.
     * @param tableNames Table names.
     */
    void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
        verticalFilter = new HashSet();
        horizontalFilter = new HashSet<SimpleExpression>();
        groupSelector = new HashSet();
    }

    /**
     * Sets the vertical filter.
     * @param verticalFilters vertical filters.
     */
    public void setVerticalFilter(Iterable<Column> verticalFilters) {
        this.verticalFilter = Sets.newHashSet(verticalFilters);
    }

    /**
     * Sets the horizontal filter.
     * @param horizontalFilter horizontal filter.
     */
    public void setHorizontalFilter(Iterable<SimpleExpression> horizontalFilter) {
        this.horizontalFilter = Sets.<SimpleExpression>newHashSet(horizontalFilter);
    }

    /**
     * Sets the group column.
     * @param column group column.
     */
    public void setGroupColumn(Column column) {
        this.groupColumn = column;
    }

    /**
     * Gets iterator class name.
     * @return iterator class name.
     */
    public String getIteratorClass() {
        return iteratorClass;
    }

    /**
     * Default group operation.
     * @param tupleCollection input tuple
     * @return a group of tuple collection.
     */
    @Override
    public Collection<TupleCollection> group(TupleCollection tupleCollection) {
        LinkedList<TupleCollection> groups = new LinkedList();

        if (groupColumn != null) {
            tracer.verbose("Use default grouping on " + groupColumn.getFullAttributeName());
            SqlQueryBuilder query = tupleCollection.getSQLQuery();
            query.addOrder(groupColumn.getFullAttributeName());

            Tuple lastTuple = null;
            List<Tuple> cur = new ArrayList();
            for (int i = 0; i < tupleCollection.size(); i ++) {
                Tuple tuple = tupleCollection.get(i);
                if (lastTuple == null) {
                    lastTuple = tuple;
                    cur.add(tuple);
                    continue;
                }

                // check whether the current tuple is within the same current group.
                Object lastValue = lastTuple.get(groupColumn);
                Object newValue = tuple.get(groupColumn);
                if (!lastValue.equals(newValue)) {
                    groups.add(new TupleCollection(cur));
                    cur = new ArrayList();
                    cur.add(tuple);
                } else {
                    cur.add(tuple);
                }

                lastTuple = tuple;
            }
            groups.add(new TupleCollection(cur));
        } else {
            groups.add(tupleCollection);
        }
        return groups;
    }

    /**
     * Default filter operation.
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public TupleCollection filter(TupleCollection tupleCollection) {
        SqlQueryBuilder query = tupleCollection.getSQLQuery();

        query.addSelect("tid");
        if (verticalFilter.size() != 0) {
            tracer.verbose("Use default vertical filter on ");
            List<Column> selects = Lists.newArrayList(verticalFilter);
            for (Column column : selects) {
                tracer.verbose(column.getFullAttributeName());
                query.addSelect(column.getFullAttributeName());
            }
        }

        if (horizontalFilter.size() != 0) {
            tracer.verbose("Use default horizontal filter on ");
            List<SimpleExpression> wheres = Lists.newArrayList(horizontalFilter);
            for (SimpleExpression where : wheres) {
                tracer.verbose(where.toString());
                query.addWhere(where.toString());
            }
        }
        return tupleCollection;
    }

    /**
     * Sets of the iterator.
     * @param iteratorClass iterator class.
     */
    public void setIteratorClass(String iteratorClass) {
        this.iteratorClass = iteratorClass;
    }

    /**
     * Returns <code>True</code> when the rule has customized iterator.
     * @return <code>True</code> when the rule has customized iterator.
     */
    public boolean hasCustomIterator() {
        return !Strings.isNullOrEmpty(iteratorClass);
    }
}