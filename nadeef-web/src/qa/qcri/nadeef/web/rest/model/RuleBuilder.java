/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.web.rest.model;

public class RuleBuilder {
    private String name;
    private String type;
    private String code;
    private String table1;
    private String table2;
    private String javaCode;

    public RuleBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public RuleBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public RuleBuilder setCode(String code) {
        this.code = code;
        return this;
    }

    public RuleBuilder setTable1(String table1) {
        this.table1 = table1;
        return this;
    }

    public RuleBuilder setTable2(String table2) {
        this.table2 = table2;
        return this;
    }

    public RuleBuilder setJavaCode(String javaCode) {
        this.javaCode = javaCode;
        return this;
    }

    public Rule createRule() {
        return new Rule(name, type, code, table1, table2, javaCode);
    }
}