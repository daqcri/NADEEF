/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Tracer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Function Dependency (FD) Rule.
 * TODO: write more unit testing on FD rule.
 */
public class FDRule extends PairTupleRule implements TextRule {
    protected ImmutableList<Column> lhs;
    protected ImmutableList<Column> rhs;

    public FDRule(String id, List<String> tableNames, StringReader reader) {
        super(id, tableNames);
        parse(reader);
    }

    /**
     * Parse FD rule from string.
     * @param input Input stream.
     */
    @Override
    public void parse(StringReader input) {
        BufferedReader reader = new BufferedReader(input);
        Set<Column> lhsSet = new HashSet();
        Set<Column> rhsSet = new HashSet();

        // Here we assume the rule comes in with one line.
        try {
            String line = reader.readLine();
            String[] tokens = line.trim().split("\\|");
            String token;
            if (tokens.length != 2) {
                throw new IllegalArgumentException("Invalid rule description " + line);
            }
            // parse the LHS
            String[] lhsSplits = tokens[0].split(",");
            // use the first one as default table name.
            String defaultTable = tableNames.get(0);
            Column newColumn = null;
            for (int i = 0; i < lhsSplits.length; i ++) {
                token = lhsSplits[i].trim().toLowerCase();
                if (Strings.isNullOrEmpty(token)) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                if (!CommonTools.isValidColumnName(token)) {
                    newColumn = new Column(defaultTable, token);
                } else {
                    newColumn = new Column(token);
                }

                if (lhsSet.contains(newColumn)) {
                    throw new IllegalArgumentException(
                        "FD cannot have duplicated column " + newColumn.getFullAttributeName()
                    );
                }
                lhsSet.add(newColumn);
            }

            // parse the RHS
            String[] rhsSplits = tokens[1].trim().split(",");
            for (int i = 0; i < rhsSplits.length; i ++) {
                token = rhsSplits[i].trim().toLowerCase();
                if (token.isEmpty()) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                if (
                    !Strings.isNullOrEmpty(defaultTable) &&
                    !CommonTools.isValidColumnName(token)
                ) {
                    newColumn = new Column(defaultTable, token);
                } else {
                    newColumn = new Column(token);
                }

                if (rhsSet.contains(newColumn)) {
                    throw new IllegalArgumentException(
                        "FD cannot have duplicated column " + newColumn.getFullAttributeName()
                    );
                }
                rhsSet.add(newColumn);
            }

            lhs = ImmutableList.copyOf(lhsSet);
            rhs = ImmutableList.copyOf(rhsSet);

        } catch (IOException ex) {
            Tracer tracer = Tracer.getTracer(FDRule.class);
            tracer.err(ex.getMessage());
        }
    }

    /**
     * FD scope.
     * @param tupleCollections input tuple collections.
     * @return tuples after filtering.
     */
    @Override
    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollections) {
        // TODO: solve the TID issue more elegantly
        TupleCollection tupleCollection = tupleCollections.iterator().next();
        tupleCollection.project(new Column(tableNames.get(0), "tid"))
            .project(lhs)
            .project(rhs);
        return tupleCollections;
    }

    /**
     * Default group operation.
     *
     * @param tupleCollections input tuple
     * @return a group of tuple collection.
     */
    @Override
    public Collection<TuplePair> group(Collection<TupleCollection> tupleCollections) {
        ArrayList<TuplePair> result = new ArrayList();
        TupleCollection tupleCollection = Iterables.get(tupleCollections, 0);
        Collection<TupleCollection> groupResult = tupleCollection.groupOn(lhs);
        for (TupleCollection tuples : groupResult) {
            tuples.orderBy(rhs);
            for (int i = 0; i < tuples.size(); i ++) {
                for (int j = i + 1; j < tuples.size(); j ++) {
                    Tuple left = tuples.get(i);
                    Tuple right = tuples.get(j);
                    for (Column column : rhs) {
                        if (!left.get(column).equals(right.get(column))) {
                            TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                            result.add(pair);
                        }
                    }
                }
            }
        }
        return result;
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

        List<Violation> result = new ArrayList();
        for (Column column : rhs) {
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
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = Lists.newArrayList();
        List<Cell> cells = Lists.newArrayList(violation.getCells());
        HashMap<Column, Cell> candidates = Maps.newHashMap();
        int vid = violation.getVid();
        Fix fix;
        Fix.Builder builder = new Fix.Builder(violation);
        for (Cell cell : cells) {
            Column column = cell.getColumn();
            if (rhs.contains(column)) {
                if (candidates.containsKey(column)) {
                    // if the right hand is already found out in another tuple
                    Cell right = candidates.get(column);
                    fix = builder.left(cell).right(right).build();
                    result.add(fix);
                } else {
                    // it is the first time of this cell shown up, put it in the
                    // candidate and wait for the next one shown up.
                    candidates.put(column, cell);
                }
            }
        }
        return result;
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
