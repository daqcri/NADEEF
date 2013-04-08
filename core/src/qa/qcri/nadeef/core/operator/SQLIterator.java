/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.*;

import java.sql.Connection;
import java.util.ArrayList;

/**
 * SQLIterator generates tuples for the rule. It also does the optimization
 * based on the input rule hints.
 */
public class SQLIterator extends Operator<SQLIteratorInput, Tuple[]> {
    /**
     * Execute the operator.
     *
     * @param input input object.
     * @return output object.
     */
    @Override
    public Tuple[] execute(SQLIteratorInput input) {
        Rule rule = input.getRule();
        RuleHintCollection hints = rule.getHints();
        Connection conn = input.getConnection();

        ProjectHint[] projects = (ProjectHint[])hints.getHint(RuleHintType.Project);
    }
}
