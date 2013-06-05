/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Tracer is a logging tool which is used for debugging / profiling / benchmarking purpose.
 */
// TODO: adds support for log4j.
public class Tracer {

    //<editor-fold desc="Private fields">
    private static Multimap<StatType, Long> stats = LinkedHashMultimap.create();

    private String infoHeader;
    private String errHeader;
    private String verboseHeader;

    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;
    private static PrintStream console = System.out;
    //</editor-fold>

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
        DetectCallTime,
        ViolationExport,
        ViolationExportTime,
        // Detect tuple count
        DetectCount,
        // Detection thread count
        DetectThreadCount,
        // repair
        RepairTime,
        RepairCallTime,
        EQTime,
        FixExport,
        FixDeserialize,
        UpdatedCellNumber,
    }

    //<editor-fold desc="Tracer creation">
    private Tracer(Class classType) {
        infoHeader = ":INFO:" + classType.getSimpleName() + ":";
        errHeader = ":ERROR:" + classType.getSimpleName() + ":";
        verboseHeader = ":VERBOSE:" + classType.getSimpleName() + ":";
    }

    /**
     * Creates a tracer class
     * @param classType
     * @return
     */
    public static Tracer getTracer(Class classType) {
        return new Tracer(classType);
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">

    /**
     * Set the output stream.
     * @param console_
     */
    public static void setConsole(PrintStream console_) {
        console = Preconditions.checkNotNull(console_);
    }

    /**
     * Print out info message.
     * @param msg info message.
     */
    public void info(String msg) {
        info(msg, infoHeader);
    }

    /**
     * Print out verbose message.
     * @param msg message.
     */
    public void verbose(String msg) {
        verbose(msg, verboseHeader);
    }

    /**
     * Print out error message.
     * @param message error message.
     * @param ex exceptions.
     */
    public void err(String message, Exception ex) {
        console.println(errHeader + message);
        // TODO: write back the exception message into a log file.
        if (isInfoOn() && ex != null) {
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            ex.printStackTrace(printWriter);
            console.println(writer.toString());
        }
    }

    //</editor-fold>

    //<editor-fold desc="Static methods">

    /**
     * Puts values to the trace statistic entry.
     * @param statType type.
     * @param value value.
     */
    public static synchronized void putStatsEntry(StatType statType, long value) {
        stats.put(statType, value);
    }

    /**
     * Accumulate values in the trace statistic entry.
     * @param statType type.
     * @param value value.
     */
    public static synchronized void addStatsEntry(StatType statType, long value) {
        if (!stats.containsKey(statType)) {
            stats.put(statType, value);
        } else {
            Collection<Long> values = stats.get(statType);
            if (values.size() > 1) {
                throw new IllegalStateException(
                    "Entry " + statType + " is found more than once in the statistic dictionary."
                );
            }
            Long newValue = stats.get(statType).iterator().next() + value;
            stats.removeAll(statType);
            stats.put(statType, newValue);
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
        info("Repair summary:", null);
        info("Rule: " + ruleName, null);
        info("----------------------------------------------------------------", null);
        info(formatEntry(StatType.RepairCallTime, "Repair perCall time", "ms"), null);
        info(formatEntry(StatType.EQTime, "EQ time", "ms"), null);
        info(formatEntry(StatType.UpdatedCellNumber, "Cell updated", ""), null);
        info("----------------------------------------------------------------", null);

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
        info("Detection summary:", null);
        info("Rule: " + ruleName, null);
        info("----------------------------------------------------------------", null);
        info(formatEntry(StatType.HScopeTime, "HScope time", "ms"), null);
        info(formatEntry(StatType.VScopeTime, "VScope time", "ms"), null);
        info(formatEntry(StatType.Blocks, "Blocks", ""), null);
        info(formatEntry(StatType.IterationCount, "Original tuple count", ""), null);
        info(formatEntry(StatType.IteratorTime, "Iterator time", "ms"), null);
        info(formatEntry(StatType.DBLoadTime, "DB load time", "ms"), null);
        info(formatEntry(StatType.DetectTime, "Detect time", "ms"), null);
        info(formatEntry(StatType.DetectCallTime, "Detect call time", "ms"), null);
        info(formatEntry(StatType.DetectThreadCount, "Detect thread count", ""), null);
        info(formatEntry(StatType.DetectCount, "Detect tuple count", ""), null);
        info(formatEntry(StatType.ViolationExport, "Violation", ""), null);
        info(formatEntry(StatType.ViolationExportTime, "Violation export time", ""), null);

        long totalTime = 0l;
        long totalViolation = 0l;
        Collection<Long> totalTimes = stats.get(StatType.DetectTime);
        Collection<Long> totalViolations = stats.get(StatType.ViolationExport);

        for (Long tmp : totalTimes) {
            totalTime += tmp;
        }

        for (Long tmp : totalViolations) {
            totalViolation += tmp;
        }

        info("----------------------------------------------------------------", null);
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
                outputBuilder.append(String.format("%5d", tmp));
            }
            value = outputBuilder.toString();
        }
        if (!Strings.isNullOrEmpty(suffix)) {
            prefix = prefix + " (" + suffix + ")";
        }
        return String.format("%-40s %s", prefix, value);
    }

    private static void verbose(String msg, String header) {
        if (verboseFlag) {
            if (header != null) {
                console.println(header + msg);
            } else {
                console.println(msg);
            }
        }
    }

    private static void info(String msg, String header) {
        if (infoFlag) {
            if (header != null) {
                console.println(header + msg);
            } else {
                console.println(msg);
            }
        }
    }
    //</editor-fold>
}
