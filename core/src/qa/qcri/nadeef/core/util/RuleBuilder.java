/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.CFDRule;
import qa.qcri.nadeef.core.datamodel.FDRule;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.RuleType;
import qa.qcri.nadeef.core.exception.InvalidRuleException;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule Builder.
 */
public class RuleBuilder {
    private RuleType type;
    private String name;
    private List<String> tableNames;
    private List<String> values;

    //<editor-fold desc="Constructor">
    public RuleBuilder(RuleType type) {
        this.type = type;
    }

    public RuleBuilder() {}
    //</editor-fold>

    //<editor-fold desc="Builder property">
    public RuleBuilder type(RuleType type) {
        this.type = type;
        return this;
    }

    public RuleBuilder name(String name) {
        this.name = name;
        return this;
    }

    public RuleBuilder table(List<String> tables) {
        this.tableNames = tables;
        return this;
    }

    public RuleBuilder table(String table) {
        tableNames = new ArrayList<String>(2);
        tableNames.add(table);
        return this;
    }

    public RuleBuilder value(String value) {
        this.values = Lists.newArrayList(value);
        return this;
    }

    public RuleBuilder value(List<String> values) {
        this.values = values;
        return this;
    }

    public Rule build() throws InvalidRuleException {
        Joiner joiner = Joiner.on("\n").skipNulls();
        String value = joiner.join(values);
        try {
            Rule rule = null;
            switch (type) {
                case FD:
                    rule = new FDRule(name, tableNames, new StringReader(value));
                    break;
                case CFD:
                    rule = new CFDRule(name, tableNames, new StringReader(value));
                    break;
                case UDF:
                    String className = values.get(0);
                    Class udfClass = Bootstrap.loadClass(className);
                    if (!Rule.class.isAssignableFrom(udfClass)) {
                        throw
                            new IllegalArgumentException(
                                "The specified class is not a Rule class."
                            );
                    }

                    rule = (Rule)udfClass.newInstance();
                    // call internal initialization on the rule.
                    rule.initialize(name, tableNames);
            }
            return rule;
        } catch (Exception ex) {
            throw new InvalidRuleException(ex);
        }
    }
    //</editor-fold>
}

