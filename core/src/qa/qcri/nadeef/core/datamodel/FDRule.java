/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import qa.qcri.nadeef.core.util.Tracer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Function Dependency (FD) Rule.
 */
public class FDRule extends TextRule {
    protected Set<Cell> lhs;
    protected Set<Cell> rhs;

    /**
     * Constructor.
     * @param input Input stream.
     */
    public FDRule(String id, StringReader input) {
        super(id);
        parse(input);
    }

    /**
     * Parse FD rule from string.
     * @param input Input stream.
     */
    @Override
    public void parse(StringReader input) {
        BufferedReader reader = new BufferedReader(input);
        lhs = new HashSet<>(0);
        rhs = new HashSet<>(0);

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
            for (int i = 0; i < lhsSplits.length; i ++) {
                split = lhsSplits[i].trim();
                if (split.isEmpty()) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }
                lhs.add(new Cell(split));
            }

            // parse the RHS
            String[] rhsSplits = splits[1].trim().split(",");
            for (int i = 0; i < rhsSplits.length; i ++) {
                split = rhsSplits[i].trim();
                if (split.isEmpty()) {
                    throw new IllegalArgumentException("Invalid rule description " + line);
                }
                rhs.add(new Cell(split));
            }
        } catch (IOException ex) {
            Tracer tracer = Tracer.getTracer(FDRule.class);
            tracer.err(ex.getMessage());
        }
    }

    /**
     * Stupid and expensive detect method.
     * @param a left tuple
     * @param b right tuple
     * @return violation set.
     */
    @Override
    public Violation[] detect(Tuple a, Tuple b) {
        ArrayList<Violation> result = new ArrayList<>(0);
        Cell[] lhsCells = lhs.toArray(new Cell[lhs.size()]);
        boolean hasSameLhs = true;
        for (Cell cell : lhsCells) {
            Object value1 = a.get(cell);
            Object value2 = b.get(cell);
            if (!value1.equals(value2)) {
                hasSameLhs = false;
                break;
            }
        }

        if (hasSameLhs) {
            Cell[] rhsCells = rhs.toArray(new Cell[rhs.size()]);
            for (Cell cell : rhsCells) {
                Object value1 = a.get(cell);
                Object value2 = b.get(cell);
                if (!value1.equals(value2)) {
                    result = getViolation(a);
                    result.addAll(getViolation(b));
                    break;
                }

            }
        }

        return result.toArray(new Violation[result.size()]);
    }

    /**
     * Detect rule with multiple tuples.
     *
     * @param tuples@return Violation set.
     */
    @Override
    public Violation[] detect(Tuple[] tuples) {
        ArrayList<Violation> violationList = new ArrayList<>();
        for (Tuple tuple : tuples) {
            violationList.addAll(getViolation(tuple));
        }
        return violationList.toArray(new Violation[violationList.size()]);
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

    private ArrayList<Violation> getViolation(Tuple tuple) {
        Cell[] cells = tuple.getCells();
        ArrayList<Violation> result = new ArrayList<>();
        for (Cell cell : cells) {
            // Only adds RHS in the violation.
            if (lhs.contains(cell)) {
                continue;
            }

            Violation violation = new Violation();
            // TODO: find a way to get tuple id.
            // violation.setTupleId();
            violation.setRuleId(this.id);
            violation.setCell(cell);
            violation.setAttributeValue(tuple.get(cell));
            result.add(violation);
        }
        return result;
    }
}
