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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.web.sql.SQLDialectBase;
import spark.Request;
import spark.Response;
import spark.Route;

import java.sql.*;

import static spark.Spark.*;

/**
 * Start class for launching dashboard.
 */
public final class Dashboard {
    private static Tracer tracer;
    private static SQLDialectBase dialectInstance;

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
                return null;
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Violation actions">
    private static void setupViolation() {
        /**
         * Gets violation table.
         */
        get(new Route("/table/violation") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return query(dialectInstance.queryViolation(), "querying violation", true, true);
            }
        });

        delete(new Route("/table/violation") {
            @Override
            public Object handle(Request request, Response response) {
                update(dialectInstance.deleteViolation(), "Deleting violation");
                return null;
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Table actions">
    private static void setupTable() {
        get(new Route("/table/:tablename") {
            @Override
            public Object handle(Request request, Response response) {
                String tableName = request.params("tablename");
                response.type("application/json");
                return query(dialectInstance.queryTable(tableName), "querying table", true, true);
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
                return query(dialectInstance.queryRule(), "querying rules", true, true);
            }
        });

        get(new Route("/data/rule/:ruleName") {
            @Override
            public Object handle(Request request, Response response) {
                String ruleName = request.params("ruleName");
                response.type("application/json");
                return query(
                    dialectInstance.queryRule(ruleName), "querying rule " + ruleName, true, true);
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
                    || Strings.isNullOrEmpty(table2)
                    || Strings.isNullOrEmpty(code)) {
                    return fail("Input cannot be null.");
                }

                int typecode = 0;
                switch (type) {
                    case "UDF":
                        typecode = 0;
                        break;
                    case "FD":
                        typecode = 1;
                        break;
                    case "CFD":
                        typecode = 2;
                        break;
                }

                // Doing a delete and insert
                update(dialectInstance.deleteRule(name), "update rule");

                update(
                    dialectInstance.insertRule(typecode, code, table1, table2, name),
                    "insert rule"
                );
                return success(0);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="source actions">
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
                        if (tableName.startsWith("csv_")) {
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
    private static void setupWidget() {
        get(new Route("/widget/attribute") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return query(dialectInstance.queryAttribute(), "querying attribute", true, true);
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
                        true,
                        true
                    );
            }
        });

        get(new Route("/widget/top10") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                return query(dialectInstance.queryTopK(10), "querying top 10", true, true);
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
                ResultSet rs = null, rs2 = null;
                try {
                    conn = DBConnectionFactory.getNadeefConnection();
                    stat = conn.createStatement();
                    rs = stat.executeQuery(dialectInstance.queryDistinctTable());
                    String tableName;
                    int sum = 0;
                    while (rs.next()) {
                        tableName = rs.getString(1);
                        rs2 = stat.executeQuery(dialectInstance.countTable(tableName));
                        if (rs2.next()) {
                            sum += rs2.getInt(1);
                        }
                        rs2.close();
                    }
                    rs2 = stat.executeQuery(dialectInstance.countViolation());
                    int violated = rs2.getInt(1);
                    result.add(sum);
                    result.add(violated);
                    json.put("data", result);
                } catch (SQLException ex) {
                    tracer.err("querying source", ex);
                    return null;
                } finally {
                    try {
                        if (rs != null) {
                            rs.close();
                        }

                        if (rs2 != null) {
                            rs2.close();
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

    //<editor-fold desc="Rule actions">
    private static void setupProgress() {
        get(new Route("/data/progress") {
            @Override
            public Object handle(Request request, Response response) {
                return null;
            }
        });
    }
    //</editor-fold>

    public static void main(String[] args) {
        Bootstrap.start();
        tracer = Tracer.getTracer(Dashboard.class);
        dialectInstance = DBInstaller.dialectInstance;

        externalStaticFileLocation("/home/si/qcri/github/trunk/web/src/public");

        setupHome();
        setupRule();
        setupTable();
        setupViolation();
        setupSource();
        setupWidget();
        setupProgress();
    }

    //<editor-fold desc="Private helpers">
    private static String query(
        String sql,
        String err,
        boolean fetechAll,
        boolean includeHeader
    ) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            conn.setAutoCommit(true);
            Statement stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            return queryToJson(rs, fetechAll, includeHeader);
        } catch (SQLException ex) {
            tracer.err(err, ex);
            rs = null;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
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
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            conn.setAutoCommit(true);
            Statement stat = conn.createStatement();
            stat.execute(sql);
        } catch (SQLException ex) {
            tracer.err(err, ex);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {}
            }
        }
    }

    private static String queryToJson(
        ResultSet rs,
        boolean fetchAll,
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
            if (!fetchAll) {
                break;
            }
        }

        result.put("data", data);
        return result.toJSONString();
    }

    private static String success(int value) {
        JSONObject obj = new JSONObject();
        obj.put("data", value);
        return obj.toJSONString();
    }

    private static String fail(String err) {
        JSONObject obj = new JSONObject();
        obj.put("error", err);
        return obj.toJSONString();
    }

    //</editor-fold>
}