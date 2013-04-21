/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.util.SqlQueryBuilder;

import java.util.*;

/**
 * Abstract base class for a rule providing the default behavior of filter and group operation.
 */
// TODO: adds generic verification to limit the T type.
public abstract class Rule<T> extends AbstractRule<T> {
    private Set<Column> verticalFilter;
    private Set<String> horizontalFilter;
    private Column groupColumn;
    private Set<String> groupSelector;

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected Rule(String id, List<String> tableNames) {
        super(id, tableNames);
        verticalFilter = new HashSet();
        horizontalFilter = new HashSet();
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
    public void setHorizontalFilter(Iterable<String> horizontalFilter) {
        this.horizontalFilter = Sets.newHashSet(horizontalFilter);
    }

    /**
     * Sets the group column.
     * @param column group column.
     */
    public void setGroupColumn(Column column) {
        this.groupColumn = column;
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
        List<Column> selects = Lists.newArrayList(verticalFilter);
        for (Column column : selects) {
            query.addSelect(column.getFullAttributeName());
            query.addDistinct(column.getFullAttributeName());
        }

        List<String> wheres = Lists.newArrayList(horizontalFilter);
        for (String where : wheres) {
            query.addWhere(where);
        }
        return tupleCollection;
    }
}