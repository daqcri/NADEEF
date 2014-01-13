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
import com.google.common.primitives.Ints;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.Tracer;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.List;

import static spark.Spark.post;

public class DedupRestServer {
    private Tracer tracer;
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

    private static void setupRest() {
        post(new Route("/do/incdedup") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String jsonString = request.body();
                JSONObject obj = (JSONObject) JSONValue.parse(jsonString);

                if (obj == null || !obj.containsKey("data")) {
                    return fail("Invalid input");
                }

                JSONArray array = (JSONArray)obj.get("data");
                List<Integer> input = Lists.newArrayList();
                for (Object t : array) {
                    input.add(Ints.checkedCast((Long)t));
                }

                try {
                    dedupClient.incrementalDedup(input);
                } catch (TException e) {
                    e.printStackTrace();
                }

                return success(0);
            }
        });
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
}
