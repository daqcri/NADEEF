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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.FileReader;
import java.nio.file.Path;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static spark.Spark.*;

public class DedupRestServer {
    private static String fileName = "nadeef.conf";
    private static DedupClient dedupClient;

    public static void main(String[] args) {
        try {
            NadeefConfiguration.initialize(new FileReader(fileName));
            // set the logging directory
            Path outputPath = NadeefConfiguration.getOutputPath();
            Tracer.setLoggingPrefix("dedup");
            Tracer.setLoggingDir(outputPath.toString());
            Tracer tracer = Tracer.getTracer(DedupRestServer.class);
            tracer.verbose("Tracer initialized at " + outputPath.toString());
            staticFileLocation("qa/qcri/nadeef/lab/dedup/web");

            // initialize nadeef client
            DedupClient.initialize(
                NadeefConfiguration.getServerUrl(),
                NadeefConfiguration.getServerPort()
            );

            dedupClient = DedupClient.getInstance();
            setupRest();
        } catch (Exception ex) {
            System.err.println("Nadeef initialization failed.");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static JSONArray flatResult(List<List<Integer>> input) {
        JSONArray result = new JSONArray();
        HashMap<Integer, HashSet<Integer>> maps = Maps.newHashMap();
        List<HashSet<Integer>> record = Lists.newArrayList();
        for (List<Integer> x : input) {
            int tid1 = x.get(0);
            int tid2 = x.get(1);

            HashSet<Integer> targetSet = null;
            if (maps.containsKey(tid1)) {
                targetSet = maps.get(tid1);
                targetSet.add(tid2);
            } else if (maps.containsKey(tid2)) {
                targetSet = maps.get(tid2);
                targetSet.add(tid1);
            }

            if (targetSet == null) {
                HashSet<Integer> nset = Sets.newHashSet();
                nset.add(tid1);
                nset.add(tid2);
                maps.put(tid1, nset);
                maps.put(tid2, nset);
                record.add(nset);
            }
        }

        int i = 0;
        for (HashSet<Integer> x : record) {
            JSONArray group = new JSONArray();
            group.add(i ++);
            for (int y : x) {
                group.add(y);
            }
            result.add(group);
        }
        return result;
    }

    private static void setupRest() {
        post(new Route("/do/incdedup") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String jsonString = request.body();
                JSONArray array = (JSONArray) JSONValue.parse(jsonString);
                if (array == null) {
                    return "[]";
                }

                List<Integer> input = Lists.newArrayList();
                for (Object t : array) {
                    input.add(Ints.checkedCast((Long)t));
                }

                List<List<Integer>> dedupPairs = null;
                JSONArray result = new JSONArray();
                try {
                    dedupPairs = dedupClient.incrementalDedup(input);
                    result = flatResult(dedupPairs);
                } catch (TException e) {
                    e.printStackTrace();
                }

                return result;
            }
        });

        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("text/html");
                response.redirect("/index.html");
                return null;
            }
        });

        get(new Route("/top") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String limit_ = request.queryParams("limit");
                int limit;
                if (limit_ == null) {
                    limit = 40;
                } else {
                    limit = Integer.parseInt(request.queryParams("limit"));
                }
                String sql = "select * from qatarcars_copy limit " + limit;
                return query(sql, true);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static JSONObject fail(String err) {
        JSONObject obj = new JSONObject();
        obj.put("error", err);
        return obj;
    }

    @SuppressWarnings("unchecked")
    private static String queryToJson(
        ResultSet rs,
        boolean includeHeader
    ) throws SQLException {
        JSONObject result = new JSONObject();
        ResultSetMetaData metaData = rs.getMetaData();
        int ncol = metaData.getColumnCount();

        if (includeHeader) {
            JSONArray columns = new JSONArray();
            for (int i = 1; i <= ncol; i ++) {
                columns.add(metaData.getColumnName(i));
            }
            result.put("schema", columns);
        }

        JSONArray data = new JSONArray();
        while (rs.next()) {
            JSONArray entry = new JSONArray();
            for (int i = 1; i <= ncol; i ++) {
                entry.add(rs.getObject(i));
            }
            data.add(entry);
        }

        return data.toJSONString();
    }

    private static String query(
        String sql,
        boolean includeHeader
    ) {
        Connection conn = null;
        ResultSet rs = null;
        Statement stat = null;
        try {
            DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            return queryToJson(rs, includeHeader);
        } catch (Exception ex) {
            return fail(ex.getMessage()).toJSONString();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (stat != null) {
                    stat.close();
                }

                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {}
        }
    }
}
