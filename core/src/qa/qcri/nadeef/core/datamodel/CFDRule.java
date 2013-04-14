/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.List;

/**
 * CFD rule.
 */
public class CFDRule extends Rule {

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected CFDRule(String id, List<String> tableNames) {
        super(id, tableNames);
    }
}
