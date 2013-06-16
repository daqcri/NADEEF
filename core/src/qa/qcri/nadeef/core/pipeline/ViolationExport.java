/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
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
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.tools.Tracer;

import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Export violations into the target place.
 */
public class ViolationExport extends Operator<Collection<Violation>, Integer> {
    private final int BULKSIZE = 1024;
    private static PushbackReader reader = new PushbackReader(new StringReader(""), 1024 * 1024);

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
        StringBuilder sb = new StringBuilder();
        Connection conn = DBConnectionFactory.getNadeefConnection();
        CopyManager copyManager = ((PGConnection)conn).getCopyAPI();
        int count = 0;

        // Copy load the data into Postgres database. It is using Postgres API.
        synchronized (ViolationExport.class) {
            // TODO: this is not out-of-process safe.
            int vid = Violations.generateViolationId();
            for (Violation violation : violations) {
                count ++;
                List<Cell> cells = Lists.newArrayList(violation.getCells());
                for (Cell cell : cells) {
                    sb.append(getSQLInsert(violation.getRuleId(), vid, cell));
                    if (count % BULKSIZE == 0) {
                        reader.unread(sb.toString().toCharArray());
                        copyManager.copyIn(
                            "COPY " +
                                NadeefConfiguration.getViolationTableName() +
                                " FROM STDIN WITH CSV",
                            reader
                        );
                        sb.delete(0, sb.length());
                    }
                }
                vid ++;
            }

            setPercentage(0.5f);
            reader.unread(sb.toString().toCharArray());
            copyManager.copyIn(
                "COPY " +
                    NadeefConfiguration.getViolationTableName() +
                    " FROM STDIN WITH CSV",
                reader
            );
        }

        conn.commit();
        conn.close();
        Tracer.putStatsEntry(
            Tracer.StatType.ViolationExportTime,
            stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );
        Tracer.putStatsEntry(Tracer.StatType.ViolationExport, count);
        return count;
    }

    /**
     * Converts a violation to SQL insert.
     */
    private String getSQLInsert(String ruleId, int vid, Cell cell) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(vid);
        sqlBuilder.append(',');
        sqlBuilder.append(ruleId);
        sqlBuilder.append(',');
        Column column = cell.getColumn();
        sqlBuilder.append(column.getTableName());
        sqlBuilder.append(',');
        sqlBuilder.append(cell.getTupleId());
        sqlBuilder.append(',');
        sqlBuilder.append(column.getColumnName());
        sqlBuilder.append(',');
        Object value = cell.getValue();
        if (value == null) {
            sqlBuilder.append("null");
        } else {
            sqlBuilder.append(value.toString());
        }
        sqlBuilder.append('\n');
        return sqlBuilder.toString();
    }
}
