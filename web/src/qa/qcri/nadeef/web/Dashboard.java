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
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialect;
import qa.qcri.nadeef.web.rest.*;

import static spark.Spark.*;

/**
 * Start class for launching dashboard.
 */
public final class Dashboard {
    //<editor-fold desc="Where everything begins">
    public static void main(String[] args) {
        Bootstrap.start();
        Tracer tracer = Tracer.getTracer(Dashboard.class);
        String rootDir = System.getProperty("rootDir");
        if (Strings.isNullOrEmpty(rootDir)) {
            staticFileLocation("qa/qcri/nadeef/web/public");
        } else {
            externalStaticFileLocation(rootDir);
        }

        get("/", (request, response) -> {
            response.redirect("/index.html");
            return 0;
        });

        exception(Exception.class, (ex, request, response) -> {
            response.status(400);
            tracer.err("Exception happens ", ex);
            JsonObject json = new JsonObject();
            json.add("error", new JsonPrimitive(ex.getMessage()));
            response.body(json.toString());
        });

        SQLDialect dialect = NadeefConfiguration.getDbConfig().getDialect();
        WidgetAction.setup(dialect);
        TableAction.setup(dialect);
        RuleAction.setup(dialect);
        SourceAction.setup(dialect);
        RemoteAction.setup(dialect);
        ProjectAction.setup(dialect);
        AnalyticAction.setup(dialect);
    }
}