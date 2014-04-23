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
import com.google.gson.*;
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
import java.util.ArrayList;
import java.util.List;

import static spark.Spark.*;

/**
 * Start class for launching dashboard.
 */
public final class Dashboard {
    private static final String TABLE_PREFIX = "TB_";
    private static Tracer tracer;
    private static SQLDialectBase dialectInstance;
    private static SQLDialect dialect;
    private static NadeefClient nadeefClient;

    private interface Ido {
        public JsonObject ido(Request request) throws Exception;
    }

    private static Object doIt(Request request, Response response, Ido doInstance) {
        response.type("application/json");
        try {
            JsonObject result = doInstance.ido(request);
            if (result != null)
                return result;
            return success(0);
        } catch (Exception ex) {
            response.status(400);
            tracer.err("Exception: ", ex);
            return fail(ex.getMessage());
        }
    }

    //<editor-fold desc="Home page">
    private static void setupHome() {
        /**
         * Start page.
         */
        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
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
            @Override public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override public JsonObject ido(Request request) throws Exception {
                        String tableName = request.params("tablename");
                        String project = request.params("project");

                        if (Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid");

                        JsonObject queryJson;
                        String start_ = request.queryParams("iDisplayStart");
                        String interval_ = request.queryParams("iDisplayLength");
                        String firstNViolation = request.queryParams("firstNViolation");


                        if (!(
                            SQLUtil.isValidInteger(start_) &&
                            SQLUtil.isValidInteger(interval_) &&
                            SQLUtil.isValidInteger(firstNViolation)
                        )) throw new IllegalArgumentException("Input is not valid.");

                        int start = Strings.isNullOrEmpty(start_) ? 0 : Integer.parseInt(start_);
                        int interval =
                            Strings.isNullOrEmpty(interval_) ? 10 : Integer.parseInt(interval_);
                        String filter = request.queryParams("sSearch");
                        ArrayList columns = null;
                        if (filter != null) {
                            JsonObject objSchema =
                                query(project, dialectInstance.querySchema(tableName), true);
                            columns =
                                new Gson().fromJson(
                                    objSchema.getAsJsonArray("schema"),
                                    ArrayList.class
                                );
                        }

                        queryJson =
                            query(
                                project,
                                dialectInstance.queryTable(
                                    tableName,
                                    start,
                                    interval,
                                    columns,
                                    filter
                                ),
                                true
                            );

                        JsonObject countJson =
                            query(project, dialectInstance.countTable(tableName), true);
                        JsonArray dataArray = countJson.getAsJsonArray("data");
                        int count = dataArray.get(0).getAsInt();
                        queryJson.add("iTotalRecords", new JsonPrimitive(count));
                        queryJson.add("iTotalDisplayRecords", new JsonPrimitive(count));
                        if (request.queryParams("sEcho") != null)
                            queryJson.add("sEcho", new JsonPrimitive(request.queryParams("sEcho")));
                        return queryJson;
                    }
                });
            }
        });

        get(new Route("/:project/table/:tablename/schema") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String tableName = request.params("tablename");
                        String project = request.params("project");

                        if (Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        return query(project, dialectInstance.querySchema(tableName), true);
                    }
                });
            }
        });

        delete(new Route("/:project/table/violation") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        return update(
                            project,
                            dialectInstance.deleteViolation(),
                            "Deleting violation"
                        );
                    }
                });
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Rule actions">
    private static void setupRule() {
        get(new Route("/:project/data/rule") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        return query(project, dialectInstance.queryRule(), true);
                    }
                });
            }
        });

        get(new Route("/:project/data/rule/:ruleName") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String ruleName = request.params("ruleName");
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project) || Strings.isNullOrEmpty(ruleName))
                            throw new IllegalArgumentException("Input is not valid.");

                        return query(project, dialectInstance.queryRule(ruleName), true);
                    }
                });
            }
        });

        delete(new Route("/:project/data/rule/:ruleName") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String ruleName = request.params("ruleName");
                        String project = request.params("project");

                        if (Strings.isNullOrEmpty(project) || Strings.isNullOrEmpty(ruleName))
                            throw new IllegalArgumentException("Input is not valid.");

                        return update(project, dialectInstance.deleteRule(ruleName), "delete rule");
                    }
                });
            }
        });

        post(new Route("/:project/data/rule") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
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
                            || Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        // Doing a delete and insert
                        update(project, dialectInstance.deleteRule(name), "update rule");

                        return update(
                            project,
                            dialectInstance.insertRule(
                                type.toUpperCase(), code, table1, table2, name
                            ),
                            "insert rule"
                        );
                    }
                });
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="source actions">
    private static void setupSource() {
        get(new Route("/:project/data/source") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        JsonObject json = new JsonObject();
                        JsonArray result = new JsonArray();
                        String project = request.params("project");

                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
                        dbConfig.switchDatabase(project);

                        List<String> tables = DBMetaDataTool.getTables(dbConfig);
                        for (String tableName : tables)
                            if (!tableName.equalsIgnoreCase("AUDIT") &&
                                !tableName.equalsIgnoreCase("VIOLATION") &&
                                !tableName.equalsIgnoreCase("RULE") &&
                                !tableName.equalsIgnoreCase("RULETYPE") &&
                                !tableName.equalsIgnoreCase("REPAIR") &&
                                !tableName.equalsIgnoreCase("PROJECT") &&
                                tableName.startsWith(TABLE_PREFIX))
                            result.add(new JsonPrimitive(tableName));
                        json.add("data", result);
                        return json;
                    }
                });
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Widget actions">
    private static void setupWidget() {
        get(new Route("/:project/widget/attribute") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        return query(project, dialectInstance.queryAttribute(), true);
                    }
                });
            }
        });

        get(new Route("/:project/widget/rule") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid");
                        return query(project, dialectInstance.queryRuleDistribution(), true);
                    }
                });
            }
        });

        get(new Route("/:project/widget/top10") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid");
                        return query(project, dialectInstance.queryTopK(10), true);
                    }
                });
            }
        });

        get(new Route("/:project/widget/violation_relation") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        JsonObject countJson =
                            query(project, dialectInstance.countTable("violation"), true);
                        JsonArray dataArray = countJson.getAsJsonArray("data");
                        int count = dataArray.get(0).getAsInt();
                        if (count > 10000)
                            return success(1);
                        return query(project, dialectInstance.queryViolationRelation(), true);
                    }
                });
            }
        });

        get(new Route("/:project/widget/overview") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.params("project");
                        Connection conn = null;
                        Statement stat = null;
                        ResultSet rs = null;
                        if (Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        JsonObject json = new JsonObject();
                        JsonArray result = new JsonArray();

                        try {
                            DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
                            dbConfig.switchDatabase(project);
                            conn = DBConnectionPool.createConnection(dbConfig, true);
                            stat = conn.createStatement();
                            rs = stat.executeQuery(dialectInstance.queryDistinctTable());
                            List<String> tableNames = Lists.newArrayList();
                            while (rs.next())
                                tableNames.add(rs.getString(1));
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
                            result.add(new JsonPrimitive(sum));
                            if (rs.next())
                                result.add(new JsonPrimitive(rs.getInt(1)));
                            json.add("data", result);
                            return json;
                        } finally {
                            if (rs != null)
                                rs.close();
                            if (stat != null)
                                stat.close();
                            if (conn != null)
                                conn.close();
                        }
                    }
                });
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Project actions">
    private static JsonObject createProject(String project) throws Exception {
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
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String dbName = NadeefConfiguration.getDbConfig().getDatabaseName();
                        return query(dbName, "SELECT dbname FROM PROJECT", true);
                    }
                });
            }
        });

        post(new Route("/project") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String project = request.queryParams("project");
                        if (Strings.isNullOrEmpty(project) || !SQLUtil.isValidTableName(project))
                            throw new IllegalArgumentException("Invalid project name " + project);
                        return createProject(project);
                    }
                });
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Do actions">
    private static void setupAction() {
        get(new Route("/progress") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String msg = nadeefClient.getJobStatus();
                        return new Gson().fromJson(msg, JsonElement.class).getAsJsonObject();
                    }
                });
            }
        });

        post(new Route("/do/generate") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String type = request.queryParams("type");
                        String name = request.queryParams("name");
                        String code = request.queryParams("code");
                        String table1 = request.queryParams("table1");
                        String project = request.queryParams("project");

                        if (Strings.isNullOrEmpty(type) ||
                            Strings.isNullOrEmpty(name) ||
                            Strings.isNullOrEmpty(code) ||
                            Strings.isNullOrEmpty(table1) ||
                            Strings.isNullOrEmpty(project))
                            throw new IllegalArgumentException("Input is not valid.");

                        String msg =
                            nadeefClient.generate(type, name, code, table1, project);
                        return new Gson().fromJson(msg, JsonElement.class).getAsJsonObject();
                    }
                });
            }
        });

        post(new Route("/do/verify") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
                        String type = request.queryParams("type");
                        String name = request.queryParams("name");
                        String code = request.queryParams("code");
                        String table1 = request.queryParams("table1");

                        if (Strings.isNullOrEmpty(type) ||
                            Strings.isNullOrEmpty(name) ||
                            Strings.isNullOrEmpty(code) ||
                            Strings.isNullOrEmpty(table1))
                            throw new IllegalArgumentException("Input is not valid.");

                        String msg = nadeefClient.verify(type, name, code);
                        return new Gson().fromJson(msg, JsonElement.class).getAsJsonObject();
                    }
                });
            }
        });

        post(new Route("/do/detect") {
            @Override
            public Object handle(Request request, Response response) {
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
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

                        String msg =
                            nadeefClient.detect(type, name, code, table1, table2, dbname);
                        return new Gson().fromJson(msg, JsonElement.class).getAsJsonObject();
                    }
                });
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
                    response.status(400);
                    return fail(ex.getMessage());
                } finally {
                    try {
                        reader.close();
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
                return doIt(request, response, new Ido() {
                    @Override
                    public JsonObject ido(Request request) throws Exception {
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
                            throw new IllegalArgumentException("Input is not valid.");
                        }
                        String msg =
                            nadeefClient.repair(type, name, code, table1, table2, dbName);
                        return new Gson().fromJson(msg, JsonElement.class).getAsJsonObject();
                    }
                });
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
    private static JsonObject query(String dbName, String sql, boolean includeHeader)
        throws Exception {
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

    private static JsonObject update(String dbname, String sql, String err) throws Exception {
        Connection conn = null;
        Statement stat = null;
        try {
            DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
            dbConfig.switchDatabase(dbname);
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.createStatement();
            stat.execute(sql);
            return success(0);
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

    private static JsonObject queryToJson(ResultSet rs, boolean includeHeader)
        throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int ncol = metaData.getColumnCount();
        JsonObject queryObject = new JsonObject();
        if (includeHeader) {
            JsonArray array = new JsonArray();
            for (int i = 1; i <= ncol; i ++)
                array.add(new JsonPrimitive(metaData.getColumnName(i)));

            queryObject.add("schema", array);
        }

        JsonArray data = new JsonArray();
        while (rs.next()) {
            JsonArray entry = new JsonArray();
            for (int i = 1; i <= ncol; i ++) {
                Object obj = rs.getObject(i);
                if (obj != null)
                    entry.add(new JsonPrimitive(obj.toString()));
            }
            data.add(entry);
        }

        queryObject.add("data", data);
        return queryObject;
    }

    private static JsonObject success(int value) {
        JsonObject obj = new JsonObject();
        obj.add("data", new JsonPrimitive(value));
        return obj;
    }

    private static JsonObject fail(String err) {
        JsonObject obj = new JsonObject();
        obj.add("error", new JsonPrimitive(err));
        return obj;
    }
    //</editor-fold>
}