/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * An optimization hint for shriking the tuple size.
 */
public abstract class RuleHint {

    /**
     * Parse the hint description from a string.
     * @param hintDescription
     */
    public abstract void parse(String hintDescription);

    /**
     * Constructor.
     * @param hintDescription hint description in string.
     */
    protected RuleHint(String hintDescription) {
        parse(hintDescription);
    }

    /**
     * Default constructor.
     */
    protected RuleHint() {}
}
