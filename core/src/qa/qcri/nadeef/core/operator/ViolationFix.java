/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Operator which executes the repair of a rule.
 */
public class ViolationFix
    extends Operator<Collection<Violation>, Collection<Fix>> {
    private Rule rule;

    public ViolationFix(Rule rule) {
        Preconditions.checkNotNull(rule);
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param violations input object.
     * @return output object.
     */
    @Override
    public Collection<Fix> execute(Collection<Violation> violations)
        throws Exception {
        LinkedList<Fix> fixes = new LinkedList<Fix>();
        for (Violation violation : violations) {
            Collection<Fix> fix = rule.repair(violation);
            fixes.addAll(fix);
        }
        return fixes;
    }
}
