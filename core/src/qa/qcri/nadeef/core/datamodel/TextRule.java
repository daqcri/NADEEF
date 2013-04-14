/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.List;

/**
 * TextRule contains rule with description text.
 */
public abstract class TextRule extends Rule {

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected TextRule(String id, List<String> tableNames) {
        super(id, tableNames);
    }

    /**
     * Interpret a rule from input text stream.
     * @param input Input stream.
     */
    public abstract void parse(StringReader input);

    /**
     * Creates the hints based on the content of the rule.
     */
    protected void createHints() {}
}
