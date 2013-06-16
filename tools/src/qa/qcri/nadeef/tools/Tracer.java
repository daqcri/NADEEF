/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package qa.qcri.nadeef.tools;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.*;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Tracer is a logging tool which is used for debugging / profiling / benchmarking purpose.
 */
// TODO: Make statistic summary generic
public class Tracer {

    //<editor-fold desc="Private fields">
    private static Map<StatType, List<Long>> stats = Maps.newHashMap();
    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;
    private static PrintStream console = System.out;
    private static String logFileName;
    private static Calendar calendar;
    private static DateFormat dateFormat;

    private Logger logger;
    //</editor-fold>

    static {
        calendar = Calendar.getInstance();
        dateFormat = new SimpleDateFormat("MMddHHmmss");
        logFileName = "log" + dateFormat.format(calendar.getTime()) + ".txt";
    }

    /**
     * Statistic entry type.
     */
    public enum StatType {
        // Detection time
        DetectTime,
        // Horizontal Scope Time
        HScopeTime,
        // Vertical Scope Time
        VScopeTime,
        // Number of tuples after Scope
        AfterScopeTuple,
        // Number of blocks after Block
        Blocks,
        // Iteration time
        IteratorTime,
        // Iteration Tuple count
        IterationCount,
        // DB load time
        DBLoadTime,
        // Detect per call time
        DetectCallTime,
        // Violation export count
        ViolationExport,
        // Violation export time
        ViolationExportTime,
        // Detect tuple count
        DetectCount,
        // Detection thread count
        DetectThreadCount,
        // repair time
        RepairTime,
        // Repair per call time
        RepairCallTime,
        // EQ time
        EQTime,
        // Candidate fix export count
        FixExport,
        // Candidate fix export time
        FixDeserialize,
        // Update cell number
        UpdatedCellNumber,
    }

    //<editor-fold desc="Tracer creation">
    private Tracer(Class classType) {
        Preconditions.checkNotNull(classType);
        logger = Logger.getLogger(classType);
    }

    /**
     * Creates a tracer class
     * @param classType input class type.
     * @return Tracer instance.
     */
    public static Tracer getTracer(Class classType) {
        return new Tracer(classType);
    }

    //</editor-fold>

    //<editor-fold desc="Public methods">

    /**
     * Initialize the logging directory.
     * @param outputPathName output logging directory.
     */
    public static void setLoggingDir(String outputPathName) {
        File outputPath = new File(outputPathName);

        if (!outputPath.exists() || !outputPath.isDirectory()) {
            console.println("Output path is not a valid directory.");
            return;
        }

        String outputFile = outputPath + File.separator + logFileName;
        try {
            FileAppender logFile =
                new FileAppender(
                    new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN),
                    outputFile
                );
            BasicConfigurator.configure(logFile);
        } catch (IOException e) {
            Tracer tracer = getTracer(Tracer.class);
            tracer.info("Cannot open log file : " + logFileName);
        }
    }

    /**
     * Set the output stream.
     * @param console_ input console print stream.
     */
    public static void setConsole(PrintStream console_) {
        console = Preconditions.checkNotNull(console_);
    }

    /**
     * Print out info message.
     * @param msg info message.
     */
    public void info(String msg) {
        if (isInfoOn()) {
            console.println(msg);
        }
        logger.info(msg);
    }

    /**
     * Print out verbose message.
     * @param msg message.
     */
    public void verbose(String msg) {
        if (isVerboseOn()) {
            console.println(msg);
            logger.debug(msg);
        }
    }

    /**
     * Print out error message.
     * @param message error message.
     * @param ex exceptions.
     */
    public void err(String message, Exception ex) {
        if (!Strings.isNullOrEmpty(message)) {
            console.println("Error: " + message);
            console.println("Exception: " + ex.getClass().getName() + ": " + ex.getMessage());
        }
        logger.error(message, ex);
    }

    //</editor-fold>

    //<editor-fold desc="Static methods">

    /**
     * Puts values to the trace statistic entry.
     * @param statType type.
     * @param value value.
     */
    public static synchronized void putStatsEntry(StatType statType, long value) {
        if (stats.containsKey(statType)) {
            List<Long> values = stats.get(statType);
            values.add(value);
        } else {
            List<Long> values = Lists.newArrayList();
            values.add(value);
            stats.put(statType, values);
        }
    }

    /**
     * Accumulate values in the trace statistic entry.
     * @param statType type.
     * @param value value.
     */
    public static synchronized void addStatsEntry(StatType statType, long value) {
        if (!stats.containsKey(statType)) {
            putStatsEntry(statType, value);
        } else {
            List<Long> values = stats.get(statType);
            if (values.size() > 1) {
                throw new IllegalStateException(
                    "Entry " + statType + " is found more than once in the statistic dictionary."
                );
            }
            Long newValue = values.get(0) + value;
            values.set(0, newValue);
        }
    }


    /**
     * Turn on / off info printing flag.
     * @param mode on / off.
     */
    public static void setInfo(boolean mode) {
        infoFlag = mode;
    }

    /**
     * Turn on / off verbose printing flag.
     * @param mode on / off.
     */
    public static void setVerbose(boolean mode) {
        verboseFlag = mode;
        Logger root = Logger.getRootLogger();
        if (verboseFlag) {
            root.setLevel(Level.DEBUG);
        } else {
            root.setLevel(Level.INFO);
        }
    }

    /**
     * Returns <code>True</code> when Info flag is on.
     * @return <code>True</code> when Info flag is on.
     */
    public static boolean isInfoOn() {
        return infoFlag;
    }

    /**
     * Returns <code>True</code> when Verbose flag is on.
     * @return <code>True</code> when Verbose flag is on.
     */
    public static boolean isVerboseOn() {
        return verboseFlag;
    }

    /**
     * Clear out the current statistic records.
     */
    public static void clearStats() {
        stats.clear();
    }

    /**
     * Print Repair summary.
     * @param ruleName rule name.
     */
    public static void printRepairSummary(String ruleName) {
        Tracer tracer = getTracer(Tracer.class);
        tracer.info("Repair summary:");
        tracer.info("Rule: " + ruleName);
        tracer.info("----------------------------------------------------------------");
        tracer.info(formatEntry(StatType.RepairCallTime, "Repair perCall time", "ms"));
        tracer.info(formatEntry(StatType.EQTime, "EQ time", "ms"));
        tracer.info(formatEntry(StatType.UpdatedCellNumber, "Cell updated", ""));
        tracer.info("----------------------------------------------------------------");

        Collection<Long> totalTimes = stats.get(StatType.RepairTime);
        Collection<Long> totalChangedCells = stats.get(StatType.UpdatedCellNumber);

        Long totalTime = 0l;
        Long totalChangedCell = 0l;
        for (Long tmp : totalTimes) {
            totalTime += tmp;
        }

        for (Long tmp : totalChangedCells) {
            totalChangedCell += tmp;
        }
        console.println(
            "Repair finished in " + totalTime + " ms " +
                "with " + totalChangedCell + " cells changed.\n"
        );
    }

    public static void printDetectSummary(String ruleName) {
        Tracer tracer = getTracer(Tracer.class);
        tracer.info("Detection summary:");
        tracer.info("Rule: " + ruleName);
        tracer.info("----------------------------------------------------------------");
        tracer.info(formatEntry(StatType.HScopeTime, "HScope time", "ms"));
        tracer.info(formatEntry(StatType.VScopeTime, "VScope time", "ms"));
        tracer.info(formatEntry(StatType.Blocks, "Blocks", ""));
        tracer.info(formatEntry(StatType.IterationCount, "Original tuple count", ""));
        tracer.info(formatEntry(StatType.IteratorTime, "Iterator time", "ms"));
        tracer.info(formatEntry(StatType.DBLoadTime, "DB load time", "ms"));
        tracer.info(formatEntry(StatType.DetectTime, "Detect time", "ms"));
        tracer.info(formatEntry(StatType.DetectCallTime, "Detect call time", "ms"));
        tracer.info(formatEntry(StatType.DetectThreadCount, "Detect thread count", ""));
        tracer.info(formatEntry(StatType.DetectCount, "Detect tuple count", ""));
        tracer.info(formatEntry(StatType.ViolationExport, "Violation", ""));
        tracer.info(formatEntry(StatType.ViolationExportTime, "Violation export time", ""));

        long totalTime = 0l;
        long totalViolation = 0l;
        Collection<Long> totalTimes = stats.get(StatType.DetectTime);
        Collection<Long> totalViolations = stats.get(StatType.ViolationExport);

        if (totalTimes != null) {
            for (Long tmp : totalTimes) {
                totalTime += tmp;
            }
        }

        if (totalViolations != null) {
            for (Long tmp : totalViolations) {
                totalViolation += tmp;
            }
        }

        tracer.info("----------------------------------------------------------------");
        console.println(
            "Detection finished in " + totalTime + " ms " +
            "and found " + totalViolation + " violations.\n"
        );
    }

    //</editor-fold>

    //<editor-fold desc="Private Helper">
    private static String formatEntry(
        StatType type,
        String prefix,
        String suffix
    ) {
        String value;
        if (!stats.containsKey(type)) {
            value = "";
        } else {
            Collection<Long> values = stats.get(type);
            StringBuilder outputBuilder = new StringBuilder(50);
            for (Long tmp : values) {
                outputBuilder.append(String.format("%9d", tmp));
            }
            value = outputBuilder.toString();
        }
        if (!Strings.isNullOrEmpty(suffix)) {
            prefix = prefix + " (" + suffix + ")";
        }
        return String.format("%-40s %s", prefix, value);
    }

    //</editor-fold>
}
