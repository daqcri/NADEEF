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

package qa.qcri.nadeef.web.rest.impl;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import qa.qcri.nadeef.web.rest.dao.DataDao;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcDataDao implements DataDao {
    private JdbcTemplate jdbcTemplate;

    public JdbcDataDao(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public JsonObject query(
        String projectName,
        String sourceName,
        Integer start,
        Integer length,
        String filter) {
        String sql;
        String subsql;
        if (!Strings.isNullOrEmpty(filter)) {
            String firstRow = "select * from ? limit 1";
            this.jdbcTemplate.query(firstRow, new RowMapper<Object>() {
                @Override
                public Object mapRow(ResultSet resultSet, int i) throws SQLException {
                    resultSet.getM
                    return null;
                }
            });
        } else {
            sql = "select * "
        }
        return null;
    }
}
