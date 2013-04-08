/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static qa.qcri.nadeef.core.datamodel.RuleHintType.*;

/**
 * A collection of hints.
 */
public class RuleHintCollection {
    public HashMap<RuleHintType, ArrayList<RuleHint>> ruleHints;

    /**
     * Adds a hint to the collection.
     * @param hint hint.
     */
    public synchronized void add(RuleHint hint) {
        if (hint instanceof ProjectHint) {
            ArrayList<RuleHint> projects = ruleHints.get(Project);
        }
    }

    /**
     * Gets a hint based on hinttype.
     * @param hintType hint type.
     * @return a list of hints.
     * TODO: use generic to solve the casting.
     */
    public RuleHint[] getHint(RuleHintType hintType) {
        List<RuleHint> list = ruleHints.get(hintType);
        RuleHint[] result = null;
        switch (hintType) {
            case Project:
                result = list.toArray(new ProjectHint[list.size()]);
                break;
            case GroupBy:
                break;
            case Filter:
                break;
            case GroupFilter:
                break;
        }
        return result;
    }
}
