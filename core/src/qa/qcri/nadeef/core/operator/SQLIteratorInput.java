/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Rule;

import java.sql.Connection;

/**
 * Input information for {@link SQLIterator}.
 * TODO: find a general input type solution for all the operators.
 */
public class SQLIteratorInput {
    private Connection conn;
    private Rule rule;

    public Rule getRule() {
        return rule;
    }

    public void setRule(Rule rule) {
        this.rule = rule;
    }

    public Connection getConnection() {
        return conn;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public SQLIteratorInput(Rule rule, Connection conn) {
        this.rule = rule;
        this.conn = conn;
    }
}
