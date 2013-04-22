/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.util.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Function Dependency (FD) Rule.
 * TODO: write more unit testing on FD rule.
 */
public class FDRule extends TextRule<TuplePair> {
    protected Set<Column> lhs;
    protected Set<Column> rhs;

    public FDRule(String id, List<String> tableNames, StringReader input) {
        super(id, tableNames);
        parse(input);
    }

    /**
     * Parse FD rule from string.
     * @param input Input stream.
     */
    @Override
    public void parse(StringReader input) {
        BufferedReader reader = new BufferedReader(input);
        lhs = new HashSet();
        rhs = new HashSet();

        // Here we assume the rule comes in with one line.
        try {
            String line = reader.readLine();
            String[] splits = line.trim().split("\\|");
            String split;
            if (splits.length != 2) {
                throw new IllegalArgumentException("Invalid rule description " + line);
            }
            // parse the LHS
            String[] lhsSplits = splits[0].split(",");
            // use the first one as default table name.
            String defaultTable = tableNames.get(0);
            for (int i = 0; i < lhsSplits.length; i ++) {
                split = lhsSplits[i].trim().toLowerCase();
                if (Strings.isNullOrEmpty(split)) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                if (!Strings.isNullOrEmpty(defaultTable) && !Column.isValidFullAttributeName(split)) {
                    lhs.add(new Column(defaultTable, split));
                } else {
                    lhs.add(new Column(split));
                }
            }

            // parse the RHS
            String[] rhsSplits = splits[1].trim().split(",");
            for (int i = 0; i < rhsSplits.length; i ++) {
                split = rhsSplits[i].trim().toLowerCase();
                if (split.isEmpty()) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                if (!Strings.isNullOrEmpty(defaultTable) && !Column.isValidFullAttributeName(split)) {
                    rhs.add(new Column(defaultTable, split));
                } else {
                    rhs.add(new Column(split));
                }
            }
        } catch (IOException ex) {
            Tracer tracer = Tracer.getTracer(FDRule.class);
            tracer.err(ex.getMessage());
        }
    }

    /**
     * FD filter.
     * @param tupleCollection input tuple collections.
     * @return tuples after filtering.
     */
    @Override
    public TupleCollection filter(TupleCollection tupleCollection) {
        SqlQueryBuilder query = tupleCollection.getSQLQuery();

        // projection filter
        query.addSelect("tid");
        List<Column> attrs = Lists.newArrayList(lhs);
        for (Column column : attrs) {
            query.addSelect(column.getFullAttributeName());
        }

        attrs = Lists.newArrayList(rhs);
        for (Column column : attrs) {
            query.addSelect(column.getFullAttributeName());
        }

        return tupleCollection;
    }

    @Override
    // TODO: create a new view for each group
    public Collection<TupleCollection> group(TupleCollection tupleCollection) {
        LinkedList<TupleCollection> groups = new LinkedList();
        SqlQueryBuilder query = tupleCollection.getSQLQuery();
        Column[] lhsColumns = lhs.toArray(new Column[lhs.size()]);
        for (Column column : lhsColumns) {
            query.addOrder(column.getFullAttributeName());
        }

        Tuple lastTuple = null;
        List<Tuple> cur = new ArrayList();
        for (int i = 0; i < tupleCollection.size(); i ++) {
            boolean isSameGroup = true;
            Tuple tuple = tupleCollection.get(i);
            if (lastTuple == null) {
                lastTuple = tuple;
                cur.add(tuple);
                continue;
            }

            // check whether the current tuple is within the same current group.
            for (Column column : lhsColumns) {
                Object lastValue = lastTuple.get(column);
                Object newValue = tuple.get(column);
                if (!lastValue.equals(newValue)) {
                    isSameGroup = false;
                    break;
                }
            }

            if (isSameGroup) {
                cur.add(tuple);
            } else {
                groups.add(new TupleCollection(cur));
                cur = new ArrayList();
                cur.add(tuple);
            }
            lastTuple = tuple;
        }
        groups.add(new TupleCollection(cur));
        return groups;
    }

    /**
     * Stupid and expensive detect method.
     * @param tuplePair tuple pair.
     * @return violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        Tuple left = tuplePair.getLeft();
        Tuple right = tuplePair.getRight();

        Column[] rhsColumns = rhs.toArray(new Column[rhs.size()]);
        List<Violation> result = new ArrayList();
        for (Column column : rhsColumns) {
            Object lvalue = left.get(column);
            Object rvalue = right.get(column);

            if (!lvalue.equals(rvalue)) {
                Violation violation = new Violation(id);
                violation.addTuple(left);
                violation.addTuple(right);
                result.add(violation);
                break;
            }
        }

        return result;
    }

    /**
     * Gets LHS set.
     * @return lhs set.
     */
    public Set getLhs() {
        return lhs;
    }

    /**
     * Gets RHS set.
     * @return rhs set.
     */
    public Set getRhs() {
        return rhs;
    }
}
