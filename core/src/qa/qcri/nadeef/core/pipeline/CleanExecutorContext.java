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

package qa.qcri.nadeef.core.pipeline;

import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;

import java.util.HashMap;
import java.util.HashSet;

public class CleanExecutorContext {
    public HashMap<String, HashSet<Integer>> getNewTuples() {
        return newTuples;
    }

    public DBConnectionPool getConnectionPool() {
        return connectionPool;
    }

    public Rule getRule() {
        return rule;
    }

    void setNewTuples(HashMap<String, HashSet<Integer>> newTuples) {
        this.newTuples = newTuples;
    }

    void setConnectionPool(DBConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    void setRule(Rule rule) {
        this.rule = rule;
    }

    private HashMap<String, HashSet<Integer>> newTuples;
    private DBConnectionPool connectionPool;
    private Rule rule;
}
