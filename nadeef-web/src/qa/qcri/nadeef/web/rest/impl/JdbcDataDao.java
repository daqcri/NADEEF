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
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.springframework.jdbc.core.JdbcTemplate;

import qa.qcri.nadeef.web.rest.dao.DataDao;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.util.List;

public class JdbcDataDao implements DataDao {
    private JdbcTemplate jdbcTemplate;

    public JdbcDataDao(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public Integer count(String projectName, String sourceName) {
        String targetTableName = buildTargetTableName(projectName, sourceName);
        return jdbcTemplate.query(
            "select count(*) from " + targetTableName,
            (resultSet, i) -> resultSet.getInt(1)).get(0);
    }

    /**
     * Query a table portion with pagination support.
     * @param projectName project name
     * @param sourceName source name.
     * @param start start page.
     * @param length page length.
     * @param filter filter string.
     * @return JsonObject of the result.
     */
    @Override
    public JsonObject query(
        String projectName,
        String sourceName,
        Integer start,
        Integer length,
        String filter) {
        String targetTableName = buildTargetTableName(projectName, sourceName);

        JsonArray schema = new JsonArray();
        // assemble the global string search sql
        String filterSql =
            jdbcTemplate.query("select * from " + targetTableName + " limit 1",
                (resultSet, k) -> {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    StringBuffer buf =
                        new StringBuffer(Strings.isNullOrEmpty(filter) ? "" : " where ");
                    for (int i = 0; i < columnCount; i ++) {
                        String columnName = metaData.getColumnName(i + 1);
                        if (!Strings.isNullOrEmpty(filter)) {
                            if (i > 0)
                                buf.append(" or ");
                            buf.append(
                                String.format("cast(%s as text) like %%%s%%", columnName, filter));
                        }
                        schema.add(new JsonPrimitive(columnName));
                    }
                    return buf.toString();
                }).get(0);

        List<JsonArray> tuples =
            jdbcTemplate.query(
                "select * from " + targetTableName + filterSql + " limit ? offset ?",
                new Object[]{length, start},
                (rs, k) -> {
                    JsonArray entry = new JsonArray();
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        Object obj = rs.getObject(i);
                        if (obj != null)
                            entry.add(new JsonPrimitive(obj.toString()));
                        else
                            entry.add(JsonNull.INSTANCE);
                    }
                    return entry;
                });

        JsonObject result = new JsonObject();
        JsonArray data = new JsonArray();
        tuples.forEach(data::add);
        result.add("data", data);
        result.add("schema", schema);
        return result;
    }

    private String buildTargetTableName(String projectName, String sourceName) {
        return projectName + "_" + sourceName;
    }
}
