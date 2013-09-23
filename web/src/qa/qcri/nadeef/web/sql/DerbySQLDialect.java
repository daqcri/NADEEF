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

package qa.qcri.nadeef.web.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

/**
 * Derby SQL Dialect.
 */
public class DerbySQLDialect extends SQLDialectBase {
    private static STGroupFile template =
        new STGroupFile(
            "qa*qcri*nadeef*web*sql*template*DerbyTemplate.stg".replace(
                "*", "/"
            ), '$', '$');

    /**
     * {@inheritDoc}
     */
    @Override
    public STGroupFile getTemplate() {
        return template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String installRule() {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("InstallRule");
        instance.add("name", "RULE");
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String installRuleType() {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("InstallRuleType");
        instance.add("name", "RULETYPE");
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String queryTable(String tableName, int start, int interval, String filter) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("QueryViolation");
        instance.add("tablename", tableName);
        instance.add("start", start);
        instance.add("interval", interval);
        if (!Strings.isNullOrEmpty(filter)) {
            filter = "%" + filter + "%";
        } else {
            filter = "%";
        }
        instance.add("filter", filter);
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String querySchema(String tableName) {
            return "SELECT * FROM " + tableName + " FETCH FIRST 1 ROWS ONLY";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String insertRule(String type, String code, String table1, String table2, String name) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST instance = template.getInstanceOf("InsertRule");
        instance.add("name", name);
        instance.add("type", type);
        instance.add("code", code);
        instance.add("table1", table1);
        instance.add("table2", table2);
        return instance.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String queryTopK(int k) {
        return "select tupleid, count(distinct(vid)) as count from VIOLATION group by tupleid " +
            "order by count desc FETCH FIRST " + k + " ROW ONLY";
    }
}
