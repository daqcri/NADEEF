/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.CFDRule;
import qa.qcri.nadeef.core.datamodel.FDRule;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.RuleType;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule Builder.
 */
public class RuleBuilder {
    private RuleType type;
    private String name;
    private List<String> tables;
    private String value;

    public RuleBuilder() {}

    public RuleBuilder type(RuleType type) {
        this.type = type;
        return this;
    }

    public RuleBuilder name(String name) {
        this.name = name;
        return this;
    }

    public RuleBuilder table(String table) {
        tables = new ArrayList<String>(2);
        tables.add(table);
        return this;
    }

    public RuleBuilder value(String value) {
        this.value = value;
        return this;
    }

    public Rule build() {
        switch (type) {
            case FD:
                return new FDRule(name, tables, new StringReader(value));
            case CFD:
                return new CFDRule(name, tables, new StringReader(value));
            case UDF:
                return new
        }

    }
}

