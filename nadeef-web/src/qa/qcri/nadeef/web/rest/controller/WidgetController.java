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

package qa.qcri.nadeef.web.rest.controller;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import qa.qcri.nadeef.web.rest.model.Rule;
import qa.qcri.nadeef.web.rest.model.RuleBuilder;

import java.util.List;

@RestController
public class WidgetController {
    private JdbcTemplate jdbcTemplate;

    @Autowired
    public WidgetController(BasicDataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{project}/data/rule")
    public @ResponseBody List<Rule> getRules(@PathVariable String project) {
        return this.jdbcTemplate.query(
            "select * from rule where project_id = (select id from project where name = ?)",
            new String[]{project},
            (rs, i) -> new RuleBuilder()
                .setName(rs.getString("name"))
                .setType(rs.getString("type"))
                .setCode(rs.getString("code"))
                .setJavaCode(rs.getString("java_code"))
                .setTable1(rs.getString("table1"))
                .setTable2(rs.getString("table2"))
                .createRule());
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{project}/data/rule/{ruleName}")
    public @ResponseBody Rule getRule(@PathVariable String project, @PathVariable String ruleName) {
        List<Rule> rules = this.jdbcTemplate.query(
            "select * from rule where name = ? and project_name = ?",
            new String[]{ruleName, project},
            (rs, i) -> new RuleBuilder()
                .setName(rs.getString("name"))
                .setType(rs.getString("type"))
                .setCode(rs.getString("code"))
                .setJavaCode(rs.getString("java_code"))
                .setTable1(rs.getString("table1"))
                .setTable2(rs.getString("table2"))
                .createRule());
        assert rules.size() == 1;
        return rules.get(0);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/{project}/data/rule/{ruleName}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteRule(@PathVariable String project, @PathVariable String ruleName) {
        this.jdbcTemplate.update(
            "delete from rule where name = ? and project = ?", ruleName, project);
    }

    @RequestMapping(method = RequestMethod.POST, value = "/{project}/data/rule")
    @ResponseStatus(HttpStatus.CREATED)
    public void createRule(@PathVariable String project, @RequestBody Rule rule) {
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
            project);
    }
}
