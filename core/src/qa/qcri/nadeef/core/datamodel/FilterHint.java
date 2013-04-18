/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.List;

/**
 * Rule Hint : Filter
 */
public class FilterHint extends RuleHint {


    private class FilterCondition {
        public String operator;
        public String value;
        public Cell cell;
    }

    private List<FilterCondition> conditions;

    /**
     * Parse the hint description from a string.
     *
     * @param hintDescription
     */
    @Override
    public void parse(String hintDescription) {
        String[] tokens = hintDescription.split(",");
        for (String token : tokens) {
            String[] ops = token.split("\\w");
            if (ops.length != 3) {
                throw new IllegalArgumentException("Invalid hint description " + token);
            }
        }
    }
}
