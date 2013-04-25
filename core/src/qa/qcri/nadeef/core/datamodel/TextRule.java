/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.io.StringReader;
import java.util.List;

/**
 * TextRule contains rule with description text.
 */
public interface TextRule {

    /**
     * Interpret a rule from input text stream.
     * @param input Input stream.
     */
    public void parse(StringReader input);
}
