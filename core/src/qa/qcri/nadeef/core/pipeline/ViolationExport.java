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

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.SQLDialectBase;
import qa.qcri.nadeef.core.util.sql.SQLDialectFactory;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Export violations into the target place.
 *
 *
 */
public class ViolationExport extends Operator<Collection<Violation>, Integer> {
    /**
     * Constructor.
     * @param plan clean plan.
     */
    public ViolationExport(CleanPlan plan) {
        super(plan);
    }

    /**
     * Export the violation into database.
     *
     * @param violations violations.
     * @return whether the exporting is successful or not.
     */
    @Override
    public Integer execute(Collection<Violation> violations) throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        String sql;
        Connection conn = null;
        Statement stat = null;
        int count = 0;
        try {
            conn = DBConnectionPool.getNadeefConnection();
            stat = conn.createStatement();
            SQLDialectBase dialectManager =
                SQLDialectFactory.getNadeefDialectManagerInstance();

            synchronized (ViolationExport.class) {
                // TODO: this is not out-of-process safe.
                int vid = Violations.generateViolationId();
                for (Violation violation : violations) {
                    count ++;
                    List<Cell> cells = Lists.newArrayList(violation.getCells());
                    for (Cell cell : cells) {
                        // skip the tuple id
                        if (cell.hasColumnName("tid")) {
                            continue;
                        }
                        sql = dialectManager.insertViolation(violation.getRuleId(), vid, cell);
                        stat.addBatch(sql);
                    }
                    vid ++;
                }
                setPercentage(0.5f);
                stat.executeBatch();
                conn.commit();
            }

            Tracer.putStatsEntry(
                Tracer.StatType.ViolationExportTime,
                stopwatch.elapsed(TimeUnit.MILLISECONDS)
            );
            Tracer.putStatsEntry(Tracer.StatType.ViolationExport, count);
        } finally {
            if (stat != null) {
                stat.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        return count;
    }
}
