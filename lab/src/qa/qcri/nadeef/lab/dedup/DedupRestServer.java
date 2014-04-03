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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static spark.Spark.*;

public class DedupRestServer {
    private static String fileName = "lab/src/qa/qcri/nadeef/lab/dedup/nadeef.conf";
    private static final String tableName = "qatarcars";
    private static DedupClient dedupClient;

    public static void main(String[] args) {
        try {
            NadeefConfiguration.initialize(new FileReader(fileName));

            // set the logging directory
            Path outputPath = NadeefConfiguration.getOutputPath();
            Tracer.setLoggingPrefix("dedup");
            Tracer.setLoggingDir(outputPath.toString());

            // initialize nadeef client
            DedupClient.initialize(
                NadeefConfiguration.getServerUrl(),
                NadeefConfiguration.getServerPort()
            );

            dedupClient = DedupClient.getInstance();

            String rootDir = System.getProperty("rootDir");
            if (Strings.isNullOrEmpty(rootDir)) {
                staticFileLocation("qa/qcri/nadeef/web/public");
            } else {
                externalStaticFileLocation(rootDir);
            }

            setPort(4568);
            setupRest();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static void setupRest() {
        get(new Route("/do/cure") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                try {
                    dedupClient.cureMissingValue();
                } catch (TException e) {
                    e.printStackTrace();
                }

                return 0;
            }
        });

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

                List<Integer> dedupPairs = null;
                try {
                    dedupPairs = dedupClient.incrementalDedup(input);
                } catch (TException e) {
                    e.printStackTrace();
                }

                return dedupPairs;
            }
        });

        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("text/html");
                response.header("Access-Control-Allow-Origin", "*");
                response.redirect("/index.html");
                return 0;
            }
        });

        get(new Route("/imgs") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String responseText = "";
                try {
                    String id_ = request.queryParams("id");
                    if (Strings.isNullOrEmpty(id_)) {
                        throw new NullPointerException();
                    }

                    int id = Integer.parseInt(id_);
                    String sql =
                        String.format(
                            "select url from images where vehicle_id = %d", id
                        );
                    return query(sql, new QueryToJsonResultSetHandler(true));
                } catch (NumberFormatException ex) {
                    ex.printStackTrace();
                    responseText = "Invalid ID";
                } catch (NullPointerException ex) {
                    responseText = "Empty ID parameter";
                }

                return fail(responseText);
            }
        });

        get(new Route("/top") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String limit_ = request.queryParams("limit");
                String offset_ = request.queryParams("offset");
                String keyword_ = request.queryParams("keyword");

                int limit = limit_ == null ? 12 : Integer.parseInt(limit_);
                int offset = offset_ == null ? 0 : Integer.parseInt(offset_);
                String sql;

                if (keyword_ == null) {
                    sql = String.format(
                        "select c.*, d.duplicate_count from %s c right join " +
                        "(select max(id) as id, duplicate_group, count(*) as duplicate_count from " +
                        "%s group by duplicate_group order by id desc limit %d) d " +
                        "on c.id = d.id order by c.timestamp desc",
                        tableName,
                        tableName,
                        limit
                    );
                } else {
                    sql = String.format(
                        "select c.*, d.duplicate_count from %s c right join " +
                        "(select max(id) as id, duplicate_group, count(*) as duplicate_count from " +
                        "(select * from %s where title like '%%%s%%' or brand_name like '%%%s%%') e " +
                        "group by duplicate_group order by id desc limit %d) d " +
                        "on c.id = d.id order by c.timestamp desc",
                        tableName,
                        tableName,
                        keyword_,
                        keyword_,
                        limit
                    );
                }
                return query(sql, new QueryToJsonResultSetHandler(true));
            }
        });

        get(new Route("/delta") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String startId_ = request.queryParams("id");
                try {
                    int startId = Integer.parseInt(startId_);
                    String sql =
                        String.format(
                            "select count(*) as id from %s where id > %d",
                            tableName,
                            startId
                        );
                    return query(sql, new QueryToJsonResultSetHandler(true));
                } catch (NumberFormatException ex) {
                    ex.printStackTrace();
                }
                return fail("Invalid input");
            }
        });

        get(new Route("/dups") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String startId_ = request.queryParams("id");
                try {
                    int startId = Integer.parseInt(startId_);
                    String sql =
                        String.format(
                            "select url from %s where duplicate_group = %d",
                            tableName,
                            startId
                        );
                    return query(sql, new QueryToJsonResultSetHandler(true));
                } catch (NumberFormatException ex) {
                    ex.printStackTrace();
                }
                return fail("Invalid input");
            }
        });

    }

    @SuppressWarnings("unchecked")
    private static JSONObject fail(String err) {
        JSONObject obj = new JSONObject();
        obj.put("error", err);
        return obj;
    }

    private static Object query(
        String sql,
        IResultSetHandler handler
    ) {
        Connection conn = null;
        ResultSet rs = null;
        Statement stat = null;
        try {
            DBConfig dbConfig = new DBConfig(NadeefConfiguration.getDbConfig());
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            return handler.handle(rs);
        } catch (Exception ex) {
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
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
