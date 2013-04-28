/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.ArrayList;
import java.util.List;

/**
 * A Fix represents a candidate repairment regarding to a violation.
 */
public class Fix {
    private String vid;
    private List<FixExpression> candidates;

    public Fix(String vid) {
        this.vid = vid;
        candidates = new ArrayList<FixExpression>();
    }

    public void add(Cell left, Cell right, Operation operation) {
        candidates.add(new FixExpression(left, right, operation));
    }

    public void add(Cell left, String value, Operation operation) {
        candidates.add(new FixExpression(left, value, operation));
    }
}
