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
import org.qa.qcri.web.dao.SourceDao;
import org.qa.qcri.web.model.Source;

import javax.sql.DataSource;
import java.util.List;

public class JdbcSourceDao implements SourceDao {
    private JdbcTemplate jdbcTemplate;

    public JdbcSourceDao(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<Source> getSources(String projectName) {
        return this.jdbcTemplate.query(
            "select * from source",
            (resultSet, i) -> new Source(
                resultSet.getString("name"),
                resultSet.getString("url"),
                resultSet.getString("project_name"))
        );
    }
}
