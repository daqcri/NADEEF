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

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.tools.PerfReport;
import qa.qcri.nadeef.tools.Tracer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Exports Violation list to a CSV file
 */
public class ViolationExportToCSV extends Operator<Collection<Violation>, File> {
    public ViolationExportToCSV(ExecutionContext context) {
        super(context);
    }

    @Override protected File execute(Collection<Violation> violations) throws Exception {
        Tracer tracer = Tracer.getTracer(ViolationExportToCSV.class);
        Path outputPath = NadeefConfiguration.getOutputPath();
        int vid = 0;

        String filename =
            String.format("violation_%s_%d.csv",
                getCurrentContext().getConnectionPool().getSourceDBConfig().getDatabaseName(),
                System.currentTimeMillis()
            );

        File file = new File(outputPath.toFile(), filename);
        tracer.info("Export to " + filename);
        byte[] result = null;
        try (
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "rw")
        ) {
            for (Violation violation : violations) {
                Collection<Cell> cells = violation.getCells();
                for (Cell cell : cells) {
                    StringBuffer line = new StringBuffer();
                    if (cell.hasColumnName("tid"))
                        continue;
                    String value = cell.getValue() == null ? "" : cell.getValue().toString();
                    line
                        .append(vid)
                        .append(",\"")
                        .append(violation.getRuleId())
                        .append("\",\"")
                        .append(cell.getColumn().getTableName())
                        .append("\",")
                        .append(cell.getTid())
                        .append(",\"")
                        .append(cell.getColumn().getColumnName())
                        .append("\",\"")
                        .append(value)
                        .append("\"\n");
                    byte[] bytes = line.toString().getBytes();
                    output.write(bytes, 0, bytes.length);
                }
                vid ++;
            }

            result = output.toByteArray();
            MappedByteBuffer buf =
                memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, result.length);
            buf.put(result);
            PerfReport.appendMetric(
                PerfReport.Metric.ViolationExport,
                violations.size()
            );
        }
        return file;
    }
}
