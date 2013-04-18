/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Strings;
import qa.qcri.nadeef.core.util.SqlQueryBuilder;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.tools.Tracer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Function Dependency (FD) Rule.
 */
public class FDRule extends TextRule<TupleCollection> {
    protected Set<Cell> lhs;
    protected Set<Cell> rhs;

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
        lhs = new HashSet(0);
        rhs = new HashSet(0);

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
                split = lhsSplits[i].trim();
                if (Strings.isNullOrEmpty(split)) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                if (!Strings.isNullOrEmpty(defaultTable) && !Cell.isValidFullAttributeName(split)) {
                    lhs.add(new Cell(defaultTable, split));
                } else {
                    lhs.add(new Cell(split));
                }
            }

            // parse the RHS
            String[] rhsSplits = splits[1].trim().split(",");
            for (int i = 0; i < rhsSplits.length; i ++) {
                split = rhsSplits[i].trim();
                if (split.isEmpty()) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }

                if (!Strings.isNullOrEmpty(defaultTable) && !Cell.isValidFullAttributeName(split)) {
                    rhs.add(new Cell(defaultTable, split));
                } else {
                    rhs.add(new Cell(split));
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
        query.setDistinct(true);
        // projection filter
        Cell[] attrs = lhs.toArray(new Cell[lhs.size()]);
        for (Cell cell : attrs) {
            query.addSelect(cell.getFullAttributeName());
        }

        attrs = rhs.toArray(new Cell[rhs.size()]);
        for (Cell cell : attrs) {
            query.addSelect(cell.getFullAttributeName());
        }

        return tupleCollection;
    }

    @Override
    public Collection<TupleCollection> group(TupleCollection tupleCollection) {
        LinkedList<TupleCollection> groups = new LinkedList();
        SqlQueryBuilder query = tupleCollection.getSQLQuery();
        Cell[] attrs = lhs.toArray(new Cell[lhs.size()]);
        for (Cell cell : attrs) {
            query.addOrder(cell.getFullAttributeName());
        }

        Tuple lastTuple = null;
        List<Tuple> cur = new ArrayList();
        for (int i = 0; i < tupleCollection.size(); i ++) {
            Tuple tuple = tupleCollection.get(i);
            if (lastTuple == null) {
                lastTuple = tuple;
                cur.add(tuple);
                continue;
            }

            for (Cell cell : attrs) {
                Object lastValue = lastTuple.get(cell);
                Object newValue = tuple.get(cell);
                if (lastValue.equals(newValue)) {
                    cur.add(tuple);
                } else {
                    groups.add(new TupleCollection(cur));
                    cur = new ArrayList<Tuple>();
                }
            }
        }
        return groups;
    }

    /**
     * Stupid and expensive detect method.
     * @param tupleCollection tupleCollection.
     * @return violation set.
     */
    @Override
    public Collection<Violation> detect(TupleCollection tupleCollection) {
        ArrayList<Violation> result = new ArrayList();
        for (int i = 0; i < tupleCollection.size(); i ++) {
            Tuple tuple = tupleCollection.get(i);
            result.addAll(Violations.fromTuple(this.id, tuple));
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
