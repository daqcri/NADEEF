/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import qa.qcri.nadeef.core.util.Tracer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Function Dependency (FD) Rule.
 */
public class FDRule extends TextRule {
    protected String[] lhs;
    protected String[] rhs;

    /**
     * Constructor.
     * @param input Input stream.
     */
    public FDRule(String id, StringReader input) {
        super(id);
        parse(input);
        isSQLSupported = true;
    }

    /**
     * Parse FD rule from string.
     * @param input Input stream.
     */
    @Override
    public void parse(StringReader input) {
        BufferedReader reader = new BufferedReader(input);

        // Here we assume the rule comes in with one line.
        try {
            String line = reader.readLine();
            String[] splits = line.split("|");
            if (splits.length != 2) {
                throw new IllegalArgumentException("Invalid rule description");
            }
            // parse the LHS
            String[] lhsSplits = splits[0].split(",");
            lhs = new String[lhsSplits.length];
            for (int i = 0; i < lhsSplits.length; i ++) {
                lhs[i] = lhsSplits[i].trim();
            }

            // parse the RHS
            String[] rhsSplits = splits[0].split(",");
            rhs = new String[rhsSplits.length];
            for (int i = 0; i < rhsSplits.length; i ++) {
                rhs[i] = rhsSplits[i].trim();
            }
        } catch (IOException ex) {
            Tracer tracer = Tracer.getTracer(FDRule.class);
            tracer.err(ex.getMessage());
        }
    }

    /**
     * Detect rule with multiple tuples.
     *
     * @param tuples@return Violation set.
     */
    @Override
    public Violation[] detect(Tuple[] tuples) {
        if (tuples.length == 1) {
            // There is no violation when there is only one tuple.
            return null;
        }

        ArrayList<Violation> violationList = new ArrayList<>();
        for (Tuple tuple : tuples) {
            Cell[] cells = tuple.getCells();
            for (int j = 0; j < cells.length; j ++) {
                Violation violation = new Violation();
                // TODO: find a way to get tuple id.
                // violation.setTupleId();
                violation.setRuleId(this.id);
                violation.setCell(cells[j]);
                violation.setAttributeValue(tuple.get(cells[j]));
                violationList.add(violation);
            }
        }
        return violationList.toArray(new Violation[violationList.size()]);
    }
}
