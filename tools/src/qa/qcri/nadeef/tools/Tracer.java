/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.io.PrintStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;

/**
 * Tracer is used for debugging / profiling / benchmarking purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private static Map<StatType, Long> stats = Maps.newHashMap();
    private Class classType;
    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;
    private static PrintStream console = System.out;

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
        this.classType = classType;
    }

    public static Tracer getTracer(Class classType) {
        return new Tracer(classType);
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public static void setConsole(PrintStream console_) {
        console = Preconditions.checkNotNull(console_);
    }

    public void info(String msg) {
        if (infoFlag) {
            console.println(":INFO:" + classType.getSimpleName() + " : " + msg);
        }
    }

    public void verbose(String msg) {
        if (verboseFlag) {
            console.println(":VERBOSE:" + classType.getName() + " : " + msg);
        }
    }

    public void err(String message, Exception ex) {
        System.err.println(":ERROR:" + classType.getName() + " : " + message);
        // TODO: write back the exception message into a log file.
        if (isInfoOn() && ex != null) {
            ex.printStackTrace();
        }
    }

    //</editor-fold>

    //<editor-fold desc="Static methods">

    /**
     * Adds values to the trace entry.
     * @param statType type.
     * @param value value.
     */
    public static synchronized void putStatEntry(StatType statType, long value) {
        stats.put(statType, value);
    }

    public static void setInfo(boolean mode) {
        infoFlag = mode;
    }

    public static void setVerbose(boolean mode) {
        verboseFlag = mode;
    }

    public static boolean isInfoOn() {
        return infoFlag;
    }

    public static boolean isVerboseOn() {
        return verboseFlag;
    }

    public static void printRepairSummary(String ruleName) {
        out("Repair summary:");
        long totalTime = stats.get(StatType.RepairTime);
        long totalChangedCell = stats.get(StatType.UpdatedCellNumber);
        out(formatEntry(StatType.RepairTime, "Repair time", "ms"));
        out("Rule : " + ruleName);
        out("----------------------------------------------------------------");
        out(formatEntry(StatType.RepairCallTime, "Repair perCall time", "ms"));
        out(formatEntry(StatType.EQTime, "EQ time", "ms"));
        out(formatEntry(StatType.UpdatedCellNumber, "Cell updated", ""));
        out("----------------------------------------------------------------");
        console.println(
            "Repair finished in " + totalTime + " ms " +
                "with " + totalChangedCell + " cells changed.\n"
        );
    }

    public static void printDetectSummary() {
        printDetectSummary(null);
    }

    public static void printDetectSummary(String ruleName) {
        out("Detection summary:");
        out("Rule :" + ruleName);
        out("----------------------------------------------------------------");
        out(formatEntry(StatType.HScopeTime, "HScope time", "ms"));
        out(formatEntry(StatType.VScopeTime, "VScope time", "ms"));
        out(formatEntry(StatType.Blocks, "Blocks", ""));
        out(formatEntry(StatType.IterationCount, "Original tuple count", ""));
        out(formatEntry(StatType.IteratorTime, "Iterator time", "ms"));
        out(formatEntry(StatType.DBLoadTime, "DB load time", "ms"));
        out(formatEntry(StatType.DetectTime, "Detect time", "ms"));
        out(formatEntry(StatType.DetectCallTime, "Detect call time", "ms"));
        out(formatEntry(StatType.DetectThreadCount, "Detect thread count", ""));
        out(formatEntry(StatType.DetectCount, "Detect tuple count", ""));
        out(formatEntry(StatType.ViolationExport, "Violation", ""));
        out(formatEntry(StatType.ViolationExportTime, "Violation export time", ""));
        long totalTime;
        long totalViolation;
        if (stats.containsKey(StatType.DetectTime)) {
            totalTime = stats.get(StatType.DetectTime);
        } else {
            totalTime = 0l;
        }

        if (stats.containsKey(StatType.ViolationExport)) {
            totalViolation = stats.get(StatType.ViolationExport);
        } else {
            totalViolation = 0l;
        }
        out("----------------------------------------------------------------");
        console.println(
            "Detection finished in " + totalTime + " ms " +
            "and found " + totalViolation + " violations.\n"
        );
    }

    private static String formatEntry(
        StatType type,
        String prefix,
        String suffix
    ) {
        long value = stats.containsKey(type) ? stats.get(type) : 0l;
        return String.format("%-40s %10d %s", prefix, value, suffix);
    }

    private static void out(String msg) {
        if (infoFlag) {
            console.println(msg);
        }
    }
    //</editor-fold>
}
