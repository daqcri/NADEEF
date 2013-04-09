/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Violation;

/**
 * Export violations into the target place.
 */
public class ViolationExportOperator extends Operator<Violation[], Boolean> {
    /**
     * Export the violation.
     *
     * @param violations violations.
     * @return whether the exporting is successful or not.
     */
    @Override
    public Boolean execute(Violation[] violations) {
    }
}
