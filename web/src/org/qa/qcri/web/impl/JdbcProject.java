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

package org.qa.qcri.web.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.qa.qcri.web.dao.ProjectDao;

import javax.sql.DataSource;
import java.util.List;

public class JdbcProject implements ProjectDao {

    private JdbcTemplate jdbcTemplate;

    public JdbcProject(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<String> getProjects() {
        return this.jdbcTemplate.query(
            "select name from project",
            (resultSet, i) -> resultSet.getString("name")
        );
    }

    @Override
    public void create(String name) {
        this.jdbcTemplate.update("insert into project (name) values (?)", name);
    }
}
