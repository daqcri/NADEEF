/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.EnumSet;

/**
 * Abstract base class for a rule.
 */
public abstract class Rule extends Primitive {
    protected boolean isSQLSupported;
    protected EnumSet<RuleInputType> signature;
    protected RuleHintCollection hintCollection;
    protected String id;

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected Rule(String id) {
        this.id = id;
        signature = EnumSet.noneOf(RuleInputType.class);
        Class ruleClass = Rule.class;
        Class[] detect1 = {Tuple.class};
        Class[] detect2 = {Tuple.class, Tuple.class};
        Class[] detect3 = {Iterable.class};

        try {
            Class root = ruleClass.getMethod("detect", detect1).getDeclaringClass();
            if (root.getName() != "Rule") {
                signature.add(RuleInputType.One);
            }

            root = ruleClass.getMethod("detect", detect2).getDeclaringClass();
            if (root.getName() != "Rule") {
                signature.add(RuleInputType.Two);
            }

            root = ruleClass.getMethod("detect", detect3).getDeclaringClass();
            if (root.getName() != "Rule") {
                signature.add(RuleInputType.Many);
            }
        } catch (Exception ignore) {}
    }

    /**
     * Gets of rule Id.
     */
    public String getId() {
        return id;
    }

    /**
     * Sets of rule Id.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Detect rule with one tuple.
     * @param tuple input tuple.
     * @return Violation set.
     */
    public Violation[] detect(Tuple tuple) {
        throw new NotImplementedException();
    };

    /**
     * Detect rule with two tuples.
     * @param tuple1 tuple 1.
     * @param tuple2 tuple 2.
     * @return Violation set.
     */
    public Violation[] detect(Tuple tuple1, Tuple tuple2) {
        throw new NotImplementedException();
    }

    /**
     * Detect rule with multiple tuples.
     *
     * @param tuples@return Violation set.
     */
    public Violation[] detect(Tuple[] tuples) {
        throw new NotImplementedException();
    }

    /**
     * Whether this rule can be executed in SQL.
     */
    public boolean supportSQL() {
        return this.isSQLSupported;
    }

    /**
     * Whether the rule implements one tuple input.
     * @return .
     */
    public boolean supportOneInput() {
        return signature.contains(RuleInputType.One);
    }

    /**
     * Whether the rule implements two tuple inputs.
     * @return .
     */
    public boolean supportTwoInputs() {
        return signature.contains(RuleInputType.Two);
    }

    /**
     * Whether the rule implements many tuple inputs.
     * @return .
     */
    public boolean supportManyInputs() {
        return signature.contains(RuleInputType.Many);
    }

    /**
     * Getter of hint.
     * @return opitmization hints.
     */
    public RuleHintCollection getHints() {
        return this.hintCollection;
    }
}