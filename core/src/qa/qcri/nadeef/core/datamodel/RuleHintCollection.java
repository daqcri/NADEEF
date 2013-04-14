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
    private HashMap<RuleHintType, ArrayList<RuleHint>> ruleHints;

    /**
     * Constructor.
     */
    public RuleHintCollection() {
        ruleHints = new HashMap<>();
        ruleHints.put(RuleHintType.Project, new ArrayList<RuleHint>());
    }

    /**
     * Gets the size of the hints collection.
     */
    public int size(RuleHintType hintType) {
        return ruleHints.get(hintType).size();
    }

    /**
     * Adds a hint to the collection.
     * @param hint hint.
     */
    public synchronized void add(RuleHint hint) {
        if (hint instanceof ProjectHint) {
            ArrayList<RuleHint> projects = ruleHints.get(Project);
            projects.add(hint);
        }
    }

    /**
     * Gets a hint based on hinttype.
     * @param hintType hint type.
     * @return a list of hints.
     * TODO: use generic to solve the casting.
     */
    public List<RuleHint> getHint(RuleHintType hintType) {
        ArrayList<RuleHint> list = ruleHints.get(hintType);
        List<RuleHint> result = null;
        switch (hintType) {
            case Project:
                result = list;
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
