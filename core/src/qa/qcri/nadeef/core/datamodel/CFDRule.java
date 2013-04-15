/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CFD rule.
 */
public class CFDRule extends TextRule<TupleCollection> {

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected CFDRule(String id, List<String> tableNames) {
        super(id, tableNames);
    }

    /**
     * Detect rule with many tuples.
     *
     * @param tuples input tuple.
     * @return Violation collection.
     */
    @Override
    public Collection<Violation> detect(TupleCollection tuples) {
        return new ArrayList<Violation>(0);
    }

    /**
     * Interpret a rule from input text stream.
     *
     * @param input Input stream.
     */
    @Override
    public void parse(StringReader input) {
    }
}
