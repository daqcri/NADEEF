/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.tools.Tracer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * CFD rule.
 */
public class CFDRule extends PairTupleRule implements TextRule {
    protected List<Column> lhs;
    protected List<Column> rhs;
    protected List<SimpleExpression> filterExpressions;

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    public CFDRule(String id, List<String> tableNames, StringReader reader) {
        super(id, tableNames);
        parse(reader);

        lhs = new ArrayList<Column>();
        rhs = new ArrayList<Column>();
        filterExpressions = new ArrayList<SimpleExpression>();
    }

    /**
     * Scope the tuple collection.
     * @param tupleCollections input tuple collections.
     * @return
     */
    @Override
    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollections) {
        List<TupleCollection> collections = Lists.newArrayList(tupleCollections);
        TupleCollection collection = collections.get(0);
        collection.project(new Column(tableNames.get(0), "tid")).project(lhs).project(rhs);
        collection.filter(filterExpressions);
        return collections;
    }

    /**
     * Detect rule with many tuples.
     *
     * @param tuplePair input tuple.
     * @return Violation collection.
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
     * Interpret a rule from input text stream.
     *
     * @param input Input stream.
     */
    @Override
    public void parse(StringReader input) {
        BufferedReader reader = new BufferedReader(input);
        lhs = new ArrayList();
        rhs = new ArrayList();

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

                if (
                    !Strings.isNullOrEmpty(defaultTable) &&
                    !Column.isValidFullAttributeName(split)
                ) {
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

                if (
                    !Strings.isNullOrEmpty(defaultTable) &&
                    !Column.isValidFullAttributeName(split)
                ) {
                    rhs.add(new Column(defaultTable, split));
                } else {
                    rhs.add(new Column(split));
                }
            }

            // parse condition line
            // TODO: currently we recreate a new FDRule per condition line, but
            // it would have more optimizations based on a buck of lines.
            line = reader.readLine();
            splits = line.trim().split(",");
            if (splits.length != lhs.size() + rhs.size()) {
                throw new IllegalArgumentException("Invalid rule description " + line);
            }

            Column curColumn;
            for (int i = 0; i < splits.length; i ++) {
                split = splits[i].trim().toLowerCase();
                if (split.equals("_")) {
                    continue;
                }
                if (i < lhs.size()) {
                    curColumn = lhs.get(i);
                } else {
                    curColumn = rhs.get(i - lhs.size());
                }

                if (Strings.isNullOrEmpty(split)) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                filterExpressions.add(SimpleExpression.newEqual(curColumn, split));
            }
        } catch (IOException ex) {
            Tracer tracer = Tracer.getTracer(FDRule.class);
            tracer.err(ex.getMessage());
        }
    }

    /**
     * Gets LHS set.
     * @return lhs set.
     */
    public List<Column> getLhs() {
        return lhs;
    }

    /**
     * Gets RHS set.
     * @return rhs set.
     */
    public List<Column> getRhs() {
        return rhs;
    }
}
