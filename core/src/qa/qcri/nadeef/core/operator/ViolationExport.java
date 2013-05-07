/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

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
        Connection conn = DBConnectionFactory.createNadeefConnection();
        CopyManager copyManager = ((PGConnection)conn).getCopyAPI();
        int count = 0;

        // Copy load the data into Postgres database. It is using Postgres API.
        synchronized (ViolationExport.class) {
            // TODO: this is not out-of-process safe.
            int vid = Violations.generateViolationId();
            for (Violation violation : violations) {
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
                    count ++;
                }
                vid ++;
            }
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
        Tracer.addStatEntry(
            Tracer.StatType.ViolationExportTime,
            Long.toString(stopwatch.elapsed(TimeUnit.MILLISECONDS))
        );
        Tracer.addStatEntry(Tracer.StatType.ViolationExport, Integer.toString(count));
        return count;
    }

    /**
     * Converts a violation to SQL insert.
     */
    private String getSQLInsert(String ruleId, int vid, Cell cell) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(vid);
        sqlBuilder.append(", '" + ruleId + "',");
        Column column = cell.getColumn();
        sqlBuilder.append("'" + column.getTableName() + "',");
        sqlBuilder.append(cell.getTupleId());
        sqlBuilder.append(",");
        sqlBuilder.append("'" + column.getAttributeName() + "',");
        sqlBuilder.append("'" + cell.getAttributeValue().toString() + "'\n");
        return sqlBuilder.toString();
    }
}
