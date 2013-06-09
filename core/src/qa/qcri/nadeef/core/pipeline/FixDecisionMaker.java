/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import qa.qcri.nadeef.core.datamodel.Fix;

import java.util.*;

/**
 * FixDecisionMaker provides algorithm which selects the right candidate fix based on a
 * collection of @see Fix.
 */
public abstract class FixDecisionMaker extends Operator<Collection<Fix>, Collection<Fix>> {
    /**
     * Constructor.
     */
    public FixDecisionMaker() {}

    /**
     * Decides which fixes are right given a collection of candidate fixes.
     *
     * @param fixes candidate fixes.
     * @return a collection of right @see Fix.
     */
    public abstract Collection<Fix> decide(Collection<Fix> fixes);

    /**
     * Execute the operator.
     *
     * @param fixes input object.
     * @return output object.
     */
    @Override
    protected final Collection<Fix> execute(Collection<Fix> fixes) throws Exception {
        return decide(fixes);
    }
}
