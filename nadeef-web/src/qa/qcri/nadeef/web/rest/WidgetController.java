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

package qa.qcri.nadeef.web.rest;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import qa.qcri.nadeef.tools.sql.SQLDialect;
import qa.qcri.nadeef.web.rest.model.Rule;
import qa.qcri.nadeef.web.sql.SQLDialectBase;

@RestController
public class WidgetController {
    private JdbcTemplate jdbcTemplate;
    private SQLDialectBase dialectBase;

    public WidgetController(BasicDataSource dataSource, SQLDialect dialect) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.dialectBase = SQLDialectBase.createDialectBaseInstance(dialect);
    }

    @RequestMapping(
        method = RequestMethod.GET,
        value = "/{project}/data/rule")
    public Rule getRule(@PathVariable String project) {
        this.jdbcTemplate = new JdbcTemplate()
        this.jdbcTemplate.query(
            this.dialectBase.queryRule(),
    }
}
