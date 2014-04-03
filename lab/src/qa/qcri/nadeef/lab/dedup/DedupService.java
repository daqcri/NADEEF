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
import com.google.common.util.concurrent.AbstractIdleService;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.DBInstaller;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static qa.qcri.nadeef.core.util.Bootstrap.shutdown;

public class DedupService extends AbstractIdleService {
    private static String fileName = "lab/src/qa/qcri/nadeef/lab/dedup/nadeef.conf";
    private final String tableName = "vehicles";
    private static Tracer tracer = Tracer.getTracer(DedupService.class);
    private int lastId;

    @Override
    protected void startUp() throws Exception {
        Bootstrap.start(fileName);
        DedupServiceHandler client = new DedupServiceHandler();
        while (true) {
            DBInstaller.cleanExecutionDB();
            if (lastId == 0) {
                client.cureMissingValue(Lists.<Integer>newArrayList());
                Thread.sleep(5000);
                DBInstaller.cleanExecutionDB();
                client.incrementalDedup(Lists.<Integer>newArrayList());
                lastId = getMaxId();
            } else {
                List<Integer> idList = getIdList(lastId);
                if (idList != null && !idList.isEmpty()) {
                    client.cureMissingValue(idList);
                    Thread.sleep(5000);
                    DBInstaller.cleanExecutionDB();
                    client.incrementalDedup(idList);
                    lastId = idList.get(idList.size() - 1);
                }
            }
            Thread.sleep(5000);
        }
    }

    private int getMaxId() {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        int result = 0;
        try {
            conn =
                DBConnectionPool.createConnection(NadeefConfiguration.getDbConfig());
            stat = conn.prepareStatement("select max(id) as id from " + tableName);
            rs = stat.executeQuery();
            if (rs.next()) {
                result = rs.getInt("id");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
                if (stat != null)
                    stat.close();
                if (rs != null)
                    rs.close();
            } catch (SQLException ex) {
                // ignore
            }
        }
        return result;
    }

    private List<Integer> getIdList(int lastId) {
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;
        List<Integer> result = null;
        try {
            conn =
                DBConnectionPool.createConnection(NadeefConfiguration.getDbConfig());
            stat = conn.prepareStatement(
                "select id from " + tableName + " where id > ? order by id"
            );
            stat.setInt(1, lastId);
            rs = stat.executeQuery();
            result = Lists.newArrayList();
            if (rs.next()) {
                result.add(rs.getInt("id"));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
                if (stat != null)
                    stat.close();
                if (rs != null)
                    rs.close();
            } catch (SQLException ex) {
                // ignore
            }
        }
        return result;
    }

    @Override
    protected void shutDown() throws Exception {
        shutdown();
    }

    /**
     * Starts the NADEEF server.
     * @param args command line args.
     */
    public static void main(String[] args) {
        DedupService service = null;
        try {
            service = new DedupService();
            service.startUp();
            Thread.sleep(100);
        } catch (Exception ex) {
            tracer.err("Nadeef service has exception underneath.", ex);
            ex.printStackTrace();
        } finally {
            if (service != null) {
                try {
                    service.shutDown();
                } catch (Exception ex) {
                    // ignore
                }
            }
            shutdown();
        }
    }
}
