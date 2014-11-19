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

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public class JdbcRule implements RuleDao {
    private JdbcTemplate jdbcTemplate;

    public JdbcRule(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<Rule> getRules(String projectName) {
        return this.jdbcTemplate.query(
            "select * from rule where project_name = ?",
            new String[]{projectName},
            (rs, i) -> new RuleBuilder()
                .setName(rs.getString("name"))
                .setType(rs.getString("type"))
                .setCode(rs.getString("code"))
                .setJavaCode(rs.getString("java_code"))
                .setTable1(rs.getString("table1"))
                .setTable2(rs.getString("table2"))
                .createRule());
    }

    @Override
    public Rule queryRule(String ruleName, String projectName) {
        List<Rule> rules = this.jdbcTemplate.query(
            "select * from rule where name = ? and project_name = ?",
            new String[]{ruleName, projectName},
            (rs, i) -> new RuleBuilder()
                .setName(rs.getString("name"))
                .setType(rs.getString("type"))
                .setCode(rs.getString("code"))
                .setJavaCode(rs.getString("java_code"))
                .setTable1(rs.getString("table1"))
                .setTable2(rs.getString("table2"))
                .createRule());
        if (rules.size() == 0)
            return null;
        return rules.get(0);
    }

    @Override
    public void deleteRule(String ruleName, String projectName) {
        this.jdbcTemplate.update(
            "delete from rule where name = ? and project = ?", ruleName, projectName);
    }

    @Override
    public void insertRule(Rule rule, String projectName) {
        this.jdbcTemplate.update(
            "insert into rule " +
                "(name, type, code, java_code, table1, table2, project_name) " +
                "values (?, ?, ?, ?, ?, ?, ?)",
            rule.getName(),
            rule.getType(),
            rule.getCode(),
            rule.getJavaCode(),
            rule.getTable1(),
            rule.getTable2(),
            projectName);
    }
}
