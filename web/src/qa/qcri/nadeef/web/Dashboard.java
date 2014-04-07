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
import com.google.common.io.Files;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.CSVTools;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.DBMetaDataTool;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialect;
import qa.qcri.nadeef.web.sql.SQLDialectBase;
import qa.qcri.nadeef.web.sql.SQLUtil;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.*;
import java.sql.*;
import java.util.List;

import static spark.Spark.*;

/**
 * Start class for launching dashboard.
 */
public final class Dashboard {
    private static final String TABLE_PREFIX = "tb_";
    private static Tracer tracer;
    private static SQLDialectBase dialectInstance;
    private static SQLDialect dialect;
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
        get(new Route("/:project/table/:tablename") {
            @Override
            @SuppressWarnings("unchecked")
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String tableName = request.params("tablename");
                String project = request.params("project");

                if (Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                JSONObject queryJson;
                String result;
                try {
                    String start_ = request.queryParams("iDisplayStart");
                    String interval_ = request.queryParams("iDisplayLength");
                    String firstNViolation = request.queryParams("firstNViolation");
                    String ruleFilter = request.queryParams("rule");

                    if (!(
                        SQLUtil.isValidInteger(start_) &&
                        SQLUtil.isValidInteger(interval_) &&
                        SQLUtil.isValidInteger(firstNViolation) &&
                        SQLUtil.isValidTableName(ruleFilter)
                    )) {
                        return fail("Invalid Input");
                    }

                    int start = Strings.isNullOrEmpty(start_) ? 0 : Integer.parseInt(start_);
                    int interval =
                        Strings.isNullOrEmpty(interval_) ? 10 : Integer.parseInt(interval_);
                    ruleFilter = Strings.isNullOrEmpty(ruleFilter) ? "%" : ruleFilter;

                    queryJson =
                        query(
                            project,
                            dialectInstance.queryTable(
                                tableName,
                                start,
                                interval,
                                firstNViolation,
                                ruleFilter
                            ),
                            true
                        );

                    JSONObject countJson =
                        query(
                            project,
                            dialectInstance.countTable(tableName),
                            true
                        );
                    JSONArray dataArray = (JSONArray)countJson.get("data");
                    Object countObj = ((JSONArray)(dataArray.get(0))).get(0);
                    Integer count;
                    if (countObj instanceof Integer)
                        count = (Integer)countObj;
                    else
                        count = ((Long)countObj).intValue();

                    queryJson.put("iTotalRecords", count.toString());
                    queryJson.put("iTotalDisplayRecords", count.toString());
                    queryJson.put("sEcho", request.queryParams("sEcho"));
                    result = queryJson.toJSONString();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    result = fail(ex.getMessage()).toJSONString();
                }
                return result;
            }
        });

        get(new Route("/:project/table/:tablename/schema") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String tableName = request.params("tablename");
                String project = request.params("project");

                if (Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                JSONObject result =
                    query(
                        project,
                        dialectInstance.querySchema(tableName),
                        true
                    );
                return result.toJSONString();
            }
        });

        delete(new Route("/:project/table/violation") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                update(
                    project,
                    dialectInstance.deleteViolation(),
                    "Deleting violation"
                );
                return success(0);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Rule actions">
    private static void setupRule() {
        get(new Route("/:project/data/rule") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                response.type("application/json");
                return
                    query(
                        project,
                        dialectInstance.queryRule(),
                        true
                    );
            }
        });

        get(new Route("/:project/data/rule/:ruleName") {
            @Override
            public Object handle(Request request, Response response) {
                String ruleName = request.params("ruleName");
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project) || Strings.isNullOrEmpty(ruleName)) {
                    return fail("Invalid input");
                }

                response.type("application/json");
                return query(
                    project,
                    dialectInstance.queryRule(ruleName),
                    true
                );
            }
        });

        delete(new Route("/:project/data/rule/:ruleName") {
            @Override
            public Object handle(Request request, Response response) {
                String ruleName = request.params("ruleName");
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project) || Strings.isNullOrEmpty(ruleName)) {
                    return fail("Invalid input");
                }

                response.type("application/json");
                update(project, dialectInstance.deleteRule(ruleName), "delete rule");
                return success(0);
            }
        });

        post(new Route("/:project/data/rule") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.params("project");

                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String table1 = request.queryParams("table1");
                String table2 = request.queryParams("table2");
                String code = request.queryParams("code");
                if (Strings.isNullOrEmpty(type)
                    || Strings.isNullOrEmpty(name)
                    || Strings.isNullOrEmpty(table1)
                    || Strings.isNullOrEmpty(code)
                    || Strings.isNullOrEmpty(project)) {
                    return fail("Input cannot be null.");
                }

                // Doing a delete and insert
                update(project, dialectInstance.deleteRule(name), "update rule");

                update(
                    project,
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
        get(new Route("/:project/data/source") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                JSONObject json = new JSONObject();
                JSONArray result = new JSONArray();
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                try {
                    DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
                    dbConfig.switchDatabase(project);

                    List<String> tables = DBMetaDataTool.getTables(dbConfig);
                    for (String tableName : tables) {
                        if (!tableName.equalsIgnoreCase("AUDIT") &&
                            !tableName.equalsIgnoreCase("VIOLATION") &&
                            !tableName.equalsIgnoreCase("RULE") &&
                            !tableName.equalsIgnoreCase("RULETYPE") &&
                            !tableName.equalsIgnoreCase("REPAIR") &&
                            !tableName.equalsIgnoreCase("PROJECT") &&
                            tableName.startsWith(TABLE_PREFIX)
                        ) {
                            result.add(tableName);
                        }
                    }
                } catch (Exception ex) {
                    tracer.err("querying source", ex);
                    return fail(ex.getMessage());
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
        get(new Route("/:project/widget/attribute") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                response.type("application/json");
                return query(
                    project,
                    dialectInstance.queryAttribute(),
                    true
                );
            }
        });

        get(new Route("/:project/widget/rule") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                response.type("application/json");
                return
                    query(
                        project,
                        dialectInstance.queryRuleDistribution(),
                        true
                    );
            }
        });

        get(new Route("/:project/widget/top10") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                response.type("application/json");
                return
                    query(
                        project,
                        dialectInstance.queryTopK(10),
                        true
                    );
            }
        });

        get(new Route("/:project/widget/violation_relation") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                return query(
                    project,
                    dialectInstance.queryViolationRelation(),
                    true
                );
            }
        });

        get(new Route("/:project/widget/overview") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String project = request.params("project");

                if (Strings.isNullOrEmpty(project)) {
                    return fail("Invalid input");
                }

                Connection conn = null;
                Statement stat = null;
                JSONObject json = new JSONObject();
                JSONArray result = new JSONArray();
                ResultSet rs = null;
                try {
                    DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
                    dbConfig.switchDatabase(project);
                    conn = DBConnectionPool.createConnection(dbConfig);
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
                } catch (Exception ex) {
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

    //<editor-fold desc="Project actions">
    private static JSONObject createProject(String project) throws Exception {
        // create the database
        if (dialect != SQLDialect.DERBY && dialect != SQLDialect.DERBYMEMORY) {
            Connection conn = null;
            Statement stat = null;
            ResultSet rs = null;
            // lower case for non-derby db.
            String project_ = project.toLowerCase();
            try {
                DBConfig rootConfig = new DBConfig(NadeefConfiguration.getDbConfig());
                // rootConfig.switchDatabase("");)
                conn = DBConnectionPool.createConnection(rootConfig, true);
                stat = conn.createStatement();
                rs = stat.executeQuery(dialectInstance.hasDatabase(project_));
                if (!rs.next()) {
                    stat.execute(dialectInstance.createDatabase(project_));
                }
            } finally {
                try {
                    if (stat != null) {
                        stat.close();
                    }

                    if (conn != null) {
                        conn.close();
                    }

                    if (rs != null) {
                        rs.close();
                    }
                } catch (SQLException e) {}
            }
        }

        DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
        dbConfig.switchDatabase(project);

        // install the tables
        try {
            DBInstaller.install(dbConfig);
            qa.qcri.nadeef.core.util.sql.DBInstaller.install(dbConfig);
        } catch (Exception ex) {
            tracer.err(ex.getMessage(), ex);
            return fail("Installing databases failed.");
        }

        // TODO: magic string, and missing transaction.
        return update(
            NadeefConfiguration.getDbConfig().getDatabaseName(),
            dialectInstance.insertProject(project), // original
            "Creating project " + project + " failed."
        );
    }

    private static void setupProject() {
        get(new Route("/project") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String dbName = NadeefConfiguration.getDbConfig().getDatabaseName();
                return query(dbName, "SELECT * FROM PROJECT", true);
            }
        });

        post(new Route("/project") {
            @Override
            public Object handle(Request request, Response response) {
                String project = request.queryParams("project");
                if (Strings.isNullOrEmpty(project) || !SQLUtil.isValidTableName(project))
                    return fail("Invalid project name " + project);
                try {
                    return createProject(project);
                } catch (Exception ex) {
                    String err = "Creating database " + project + " failed.";
                    tracer.err(err, ex);
                    return fail(err);
                }
            }
        });
    }

    //<editor-fold desc="Do actions">
    private static void setupAction() {

        get(new Route("/progress") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String result;
                try {
                    result = nadeefClient.getJobStatus();
                } catch (Exception ex) {
                    tracer.err("Request progress failed.", ex);
                    result = fail(ex.getMessage()).toJSONString();
                }
                return result;
            }
        });

        post(new Route("/do/generate") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");

                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String code = request.queryParams("code");
                String table1 = request.queryParams("table1");
                String project = request.queryParams("project");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(table1) ||
                    Strings.isNullOrEmpty(project)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result = nadeefClient.generate(type, name, code, table1, project);
                } catch (Exception ex) {
                    tracer.err("Generate code failed.", ex);
                    result = fail(ex.getMessage()).toJSONString();
                }
                return result;
            }
        });

        post(new Route("/do/verify") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");

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
                    result = fail(ex.getMessage()).toJSONString();
                }
                return result;
            }
        });

        post(new Route("/do/detect") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");

                String type = request.queryParams("type");
                String name = request.queryParams("name");
                String code = request.queryParams("code");
                String table1 = request.queryParams("table1");
                String table2 = request.queryParams("table2");
                String dbname = request.queryParams("project");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(dbname) ||
                    Strings.isNullOrEmpty(table1)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result =
                        nadeefClient.detect(type, name, code, table1, table2, dbname);
                } catch (Exception ex) {
                    tracer.err("Detection failed.", ex);
                    result = fail(ex.getMessage()).toJSONString();
                }
                return result;
            }
        });

        post(new Route("/do/upload") {
            @Override
            public Object handle(Request request, Response response) {
                String body = request.body();
                BufferedReader reader = new BufferedReader(new StringReader(body));
                BufferedWriter writer = null;
                String line;
                try {
                    // parse the project name
                    String projectName = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.contains("project")) {
                            while ((line = reader.readLine()) != null) {
                                if (!line.isEmpty()) {
                                    projectName = line.trim();
                                    break;
                                }
                            }
                        }

                        if (projectName != null) {
                            break;
                        }
                    }

                    // parse the file name
                    int begin;
                    String fileName = null;
                    while ((line = reader.readLine()) != null) {
                        String fileNamePrefix = "filename=";
                        if (line.contains(fileNamePrefix)) {
                            begin = line.indexOf(fileNamePrefix) + fileNamePrefix.length() + 1;
                            for (int i = begin; i < line.length(); i ++) {
                                if (line.charAt(i) == '\"') {
                                    fileName = line.substring(begin, i);
                                    break;
                                }
                            }
                        }

                        if (fileName != null) {
                            break;
                        }
                    }

                    // parse until the beginning of the file
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty()) {
                            break;
                        }
                    }

                    // write to disk
                    File outputFile = File.createTempFile(TABLE_PREFIX, fileName);
                    writer = new BufferedWriter(new FileWriter(outputFile));
                    while ((line = reader.readLine()) != null) {
                        if (line.contains("multipartformboundary") || line.isEmpty()) {
                            continue;
                        }
                        writer.write(line);
                        writer.write("\n");
                    }
                    writer.flush();
                    tracer.info("Write upload file to " + outputFile.getAbsolutePath());

                    // dump into database
                    DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
                    dbConfig.switchDatabase(projectName);

                    // TODO: resolve this by using injection deps.
                    qa.qcri.nadeef.core.util.sql.SQLDialectBase dialectBase =
                        qa.qcri.nadeef.core.util.sql.SQLDialectBase.createDialectBaseInstance(
                            dbConfig.getDialect()
                        );

                    String tableName = Files.getNameWithoutExtension(fileName);
                    CSVTools.dump(
                        dbConfig,
                        dialectBase,
                        outputFile,
                        tableName,
                        NadeefConfiguration.getAlwaysOverrideTable());

                } catch (Exception ex) {
                    tracer.err("Upload file failed.", ex);
                    return fail(ex.getMessage());
                } finally {
                    try {
                        if (reader != null) {
                            reader.close();
                        }

                        if (writer != null) {
                            writer.close();
                        }
                    } catch (Exception ex) {}
                }
                return success(0);
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
                String dbName = request.queryParams("project");

                if (Strings.isNullOrEmpty(type) ||
                    Strings.isNullOrEmpty(name) ||
                    Strings.isNullOrEmpty(code) ||
                    Strings.isNullOrEmpty(dbName) ||
                    Strings.isNullOrEmpty(table1)) {
                    return fail("Input cannot be NULL.");
                }

                String result;
                try {
                    result =
                        nadeefClient.repair(type, name, code, table1, table2, dbName);
                } catch (Exception ex) {
                    tracer.err("Generate code failed.", ex);
                    result = fail(ex.getMessage()).toJSONString();
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
        dialect = NadeefConfiguration.getDbConfig().getDialect();
        dialectInstance = SQLDialectBase.createDialectBaseInstance(dialect);
        nadeefClient = Bootstrap.getNadeefClient();

        String rootDir = System.getProperty("rootDir");
        if (Strings.isNullOrEmpty(rootDir)) {
            staticFileLocation("qa/qcri/nadeef/web/public");
        } else {
            externalStaticFileLocation(rootDir);
        }

        // special call for Ruleminer demo.
        try {
            createProject("dcdemo");
        } catch (Exception ex) {
            tracer.err("dcdemo db creation failed.");
            System.exit(1);
        }

        Tracer.setInfo(true);
        setupHome();
        setupRule();
        setupTable();
        setupSource();
        setupWidget();
        setupAction();
        setupProject();
    }
    //</editor-fold>

    //<editor-fold desc="Private helpers">
    private static JSONObject query(
        String dbName,
        String sql,
        boolean includeHeader
    ) {
        Connection conn = null;
        ResultSet rs = null;
        Statement stat = null;
        try {
            DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
            dbConfig.switchDatabase(dbName);
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            return queryToJson(rs, includeHeader);
        } catch (Exception ex) {
            tracer.err("Query \n" + sql + " failed.", ex);
            return fail(ex.getMessage());
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

    private static JSONObject update(String dbname, String sql, String err) {
        Connection conn = null;
        Statement stat = null;
        try {
            DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
            dbConfig.switchDatabase(dbname);
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.createStatement();
            stat.execute(sql);
            return success(0);
        } catch (Exception ex) {
            tracer.err(err, ex);
            return fail(ex.getMessage());
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
    private static JSONObject success(int value) {
        JSONObject obj = new JSONObject();
        obj.put("data", value);
        return obj;
    }

    @SuppressWarnings("unchecked")
    private static JSONObject fail(String err) {
        JSONObject obj = new JSONObject();
        obj.put("error", err);
        return obj;
    }

    //</editor-fold>
}