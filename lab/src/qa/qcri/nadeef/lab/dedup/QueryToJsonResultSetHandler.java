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

package qa.qcri.nadeef.lab.dedup;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class QueryToJsonResultSetHandler implements IResultSetHandler {
    private boolean includeHeader;

    public QueryToJsonResultSetHandler(boolean includeHeader) {
        this.includeHeader = includeHeader;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object handle(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int ncol = metaData.getColumnCount();

        JSONArray data = new JSONArray();
        while (rs.next()) {
            JSONObject entry = new JSONObject();
            for (int i = 1; i <= ncol; i ++) {
                Object x = rs.getObject(i);
                String columnName = metaData.getColumnName(i);
                if (x instanceof Timestamp) {
                    Timestamp timestamp = (Timestamp)x;
                    String s = new SimpleDateFormat("MM/dd HH:mm:ss").format(timestamp);
                    entry.put(columnName, s);
                } else {
                    entry.put(columnName, x);
                }
            }
            data.add(entry);
        }

        return data.toJSONString();
    }
}
