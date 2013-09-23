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

package qa.qcri.nadeef.web;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.web.sql.SQLDialectBase;
import spark.Request;
import spark.Response;
import spark.Route;

import java.sql.*;
import java.util.List;

import static spark.Spark.*;

/**
 * Start class for launching dashboard.
 */
public final class Dashboard {
    private static Tracer tracer;
    private static SQLDialectBase dialectInstance;
    private static NadeefClient nadeefClient;

    //<editor-fold desc="Home page">
    private static void setupHome() {
        /**
         * Start page.
         */
        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("text/html");
                response.redirect("/index.html");
                return success(0);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Table actions">
    private static void setupTable() {
        /**
         * Gets violation table with pagination support.
         */
        get(new Route("/table/:tablename") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String tableName = request.params("tablename");
                JSONObject queryJson;
                String result;
                try {
                    String start_ = request.queryParams("iDisplayStart");
                    String interval_ = request.queryParams("iDisplayLength");
                    String filter = request.params("sSearch");
                    int start =
                        Strings.isNullOrEmpty(start_) ? 0 : Integer.parseInt(start_);
                    int interval =
                        Strings.isNullOrEmpty(interval_) ? 10 : Integer.parseInt(interval_);

                    queryJson =
                        query(
                            dialectInstance.queryTable(tableName, start, interval, filter),
                            "Query table " + tableName,
                            true
                        );

                    JSONObject countJson =
                        query(
                            dialectInstance.countTable(tableName),
                            "Query table count " + tableName,
                            true
                        );
                    JSONArray dataArray = (JSONArray)countJson.get("data");
                    Integer count = (Integer)(((JSONArray)(dataArray.get(0))).get(0));
                    queryJson.put("iTotalRecords", count.toString());
                    queryJson.put("iTotalDisplayRecords", count.toString());
                    queryJson.put("sEcho", request.queryParams("sEcho"));
                    result = queryJson.toJSONString();
                } catch (Exception ex) {
                    result = fail(ex.getMessage());
                }
                return result;
            }
        });

        get(new Route("/table/:tablename/schema") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String tableName = request.params("tablename");
                JSONObject result =
                    query(
                        dialectInstance.querySchema(tableName),
                        "Query schema " + tableName,
                        true
                    );
                return result.toJSONString();
            }
        });

        delete(new Route("/table/violation") {
            @Override
            public Object handle(Request request, Response response) {
                update(dialectInstance.deleteViolation(), "Deleting violation");
                return success(0);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Rule actions">
    private static void setupRule() {
        get(new Route("/data/rule") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return
                    query(
                        dialectInstance.queryRule(),
                        "querying rules",
                        true
                    ).toJSONString();
            }
        });

        get(new Route("/data/rule/:ruleName") {
            @Override
            public Object handle(Request request, Response response) {
                String ruleName = request.params("ruleName");
                response.type("application/json");
                return query(
                    dialectInstance.queryRule(ruleName),
                    "querying rule " + ruleName,
                    true
                ).toJSONString();
            }
        });

        post(new Route("/data/rule") {
            @Override
            public Object handle(Request request, Response response) {
                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String table1 = request.queryParams("table1");
                String table2 = request.queryParams("table2");
                String code = request.queryParams("code");

                if (Strings.isNullOrEmpty(type)
                    || Strings.isNullOrEmpty(name)
                    || Strings.isNullOrEmpty(table1)
                    || Strings.isNullOrEmpty(code)) {
                    return fail("Input cannot be null.");
                }

                // Doing a delete and insert
                update(dialectInstance.deleteRule(name), "update rule");

                update(
                    dialectInstance.insertRule(type.toUpperCase(), code, table1, table2, name),
                    "insert rule"
                );
                return success(0);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="source actions">
    @SuppressWarnings("unchecked")
    private static void setupSource() {
        get(new Route("/data/source") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                Connection conn = null;
                JSONObject json = new JSONObject();
                JSONArray result = new JSONArray();
                try {
                    conn = DBConnectionFactory.getNadeefConnection();
                    DatabaseMetaData meta = conn.getMetaData();
                    ResultSet rs = meta.getTables(null, null, null, new String[] {"TABLE"});
                    while (rs.next()) {
                        // TODO: magic number
                        String tableName = rs.getString(3);
                        if ( !tableName.equalsIgnoreCase("AUDIT") &&
                             !tableName.equalsIgnoreCase("VIOLATION") &&
                             !tableName.equalsIgnoreCase("RULE") &&
                             !tableName.equalsIgnoreCase("RULETYPE") &&
                             !tableName.equalsIgnoreCase("REPAIR")
                        ) {
                            result.add(rs.getString(3));
                        }
                    }
                } catch (SQLException ex) {
                    tracer.err("querying source", ex);
                    return null;
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException ex) {}
                    }
                }

                json.put("data", result);
                return json.toJSONString();
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Widget actions">
    @SuppressWarnings("unchecked")
    private static void setupWidget() {
        get(new Route("/widget/attribute") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return query(
                    dialectInstance.queryAttribute(),
                    "querying attribute",
                    true
                ).toJSONString();
            }
        });

        get(new Route("/widget/rule") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return
                    query(
                        dialectInstance.queryRuleDistribution(),
                        "querying rule distribution",
                        true
                    ).toJSONString();
            }
        });

        get(new Route("/widget/top10") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return
                    query(
                        dialectInstance.queryTopK(10),
                        "querying top 10",
                        true
                    ).toJSONString();
            }
        });

        get(new Route("/widget/violation_relation") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return query(
                    dialectInstance.queryViolationRelation(),
                    "querying attribute",
                    true
                ).toJSONString();
            }
        });

        get(new Route("/widget/overview") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                Connection conn = null;
                Statement stat = null;
                JSONObject json = new JSONObject();
                JSONArray result = new JSONArray();
                ResultSet rs = null;
                try {
                    conn = DBConnectionFactory.getNadeefConnection();
                    stat = conn.createStatement();
                    rs = stat.executeQuery(dialectInstance.queryDistinctTable());
                    List<String> tableNames = Lists.newArrayList();
                    while (rs.next()) {
                        tableNames.add(rs.getString(1));
                    }
                    rs.close();

                    int sum = 0;
                    for (String tableName : tableNames) {
                        rs = stat.executeQuery(dialectInstance.countTable(tableName));
                        if (rs.next()) {
                            sum += rs.getInt(1);
                        }
                        rs.close();
                    }

                    rs = stat.executeQuery(dialectInstance.countViolation());
                    result.add(sum);
                    if (rs.next()) {
                        result.add(rs.getInt(1));
                    }
                    json.put("data", result);
                } catch (SQLException ex) {
                    tracer.err("querying source", ex);
                    return null;
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
                    } catch (SQLException ex) {}
                }
                return json.toJSONString();
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Do actions">
    private static void setupAction() {
        get(new Route("/data/progress") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String result;
                try {
                    result = nadeefClient.getJobStatus();
                } catch (Exception ex) {
                    tracer.err("Request progress failed.", ex);
                    result = fail(ex.getMessage());
                }
                return result;
            }
        });

        post(new Route("/do/generate") {
            @Override
            public Object handle(Request request, Response response) {
                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String code = request.queryParams("code");
                String table1 = request.queryParams("table1");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(table1)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result = nadeefClient.generate(type, name, code, table1);
                } catch (Exception ex) {
                    tracer.err("Generate code failed.", ex);
                    result = fail(ex.getMessage());
                }
                return result;
            }
        });

        post(new Route("/do/verify") {
            @Override
            public Object handle(Request request, Response response) {
                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String code = request.queryParams("code");
                String table1 = request.queryParams("table1");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(table1)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result = nadeefClient.verify(type, name, code);
                } catch (Exception ex) {
                    tracer.err("Generate code failed.", ex);
                    result = fail(ex.getMessage());
                }
                return result;
            }
        });

        post(new Route("/do/detect") {
            @Override
            public Object handle(Request request, Response response) {
                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String code = request.queryParams("code");
                String table1 = request.queryParams("table1");
                String table2 = request.queryParams("table2");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(table1)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result = nadeefClient.detect(type, name, code, table1, table2);
                } catch (Exception ex) {
                    tracer.err("Detection failed.", ex);
                    result = fail(ex.getMessage());
                }
                return result;
            }
        });

        post(new Route("/do/repair") {
            @Override
            public Object handle(Request request, Response response) {
                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String code = request.queryParams("code");
                String table1 = request.queryParams("table1");
                String table2 = request.queryParams("table2");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(table1)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result = nadeefClient.repair(type, name, code, table1, table2);
                } catch (Exception ex) {
                    tracer.err("Generate code failed.", ex);
                    result = fail(ex.getMessage());
                }
                return result;
            }
        });

    }
    //</editor-fold>

    //<editor-fold desc="Where everything begins">
    public static void main(String[] args) {
        Bootstrap.start();
        tracer = Tracer.getTracer(Dashboard.class);
        dialectInstance = DBInstaller.dialectInstance;
        nadeefClient = Bootstrap.getNadeefClient();

        String rootDir = System.getProperty("rootDir");
        if (Strings.isNullOrEmpty(rootDir)) {
            staticFileLocation("qa/qcri/nadeef/web/public");
        } else {
            externalStaticFileLocation(rootDir);
        }

        setupHome();
        setupRule();
        setupTable();
        setupSource();
        setupWidget();
        setupAction();
    }
    //</editor-fold>

    //<editor-fold desc="Private helpers">
    private static JSONObject query(
        String sql,
        String err,
        boolean includeHeader
    ) {
        Connection conn = null;
        ResultSet rs = null;
        Statement stat = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            conn.setAutoCommit(true);
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            return queryToJson(rs, includeHeader);
        } catch (SQLException ex) {
            tracer.err(err, ex);
            rs = null;
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
        return null;
    }

    private static void update(String sql, String err) {
        Connection conn = null;
        Statement stat = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            conn.setAutoCommit(true);
            stat = conn.createStatement();
            stat.execute(sql);
        } catch (SQLException ex) {
            tracer.err(err, ex);
        } finally {
            try {
                if (stat != null) {
                    stat.close();
                }

                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {}
        }
    }

    @SuppressWarnings("unchecked")
    private static JSONObject queryToJson(
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

        result.put("data", data);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static String success(int value) {
        JSONObject obj = new JSONObject();
        obj.put("data", value);
        return obj.toJSONString();
    }

    @SuppressWarnings("unchecked")
    private static String fail(String err) {
        JSONObject obj = new JSONObject();
        obj.put("error", err);
        return obj.toJSONString();
    }

    //</editor-fold>
}