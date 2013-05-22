/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.collect.*;

import java.util.List;

/**
 * Tracer is used for debugging / profiling / benchmarking purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private static Multimap<StatType, Long> stats = ArrayListMultimap.create();
    private Class classType;
    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;

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
    public void info(String msg) {
        if (infoFlag) {
            System.out.println(":INFO:" + classType.getSimpleName() + " : " + msg);
        }
    }

    public void verbose(String msg) {
        if (verboseFlag) {
            System.out.println(":VERBOSE:" + classType.getName() + " : " + msg);
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
    public static void addStatEntry(StatType statType, long value) {
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

    public static void recreateStat() {
        stats.clear();
    }

    public static void printRepairSummary() {
        out("Repair summary:");
        List<Long> repairStats = Lists.newArrayList(stats.get(StatType.RepairTime));
        List<Long> repairCallStats = Lists.newArrayList(stats.get(StatType.RepairCallTime));
        List<Long> eqStats = Lists.newArrayList(stats.get(StatType.EQTime));
        List<Long> cellStats = Lists.newArrayList(stats.get(StatType.UpdatedCellNumber));

        long totalTime = 0;
        int totalChangedCell = 0;

        for (int i = 0; i < repairStats.size(); i ++) {
            long time = repairStats.get(i);
            out("Rule " + i + ":");
            out("----------------------------------------------------------------");
            out(String.format("%-30s %10d ms", "Repair time" , time));
            totalTime += time;

            time = repairCallStats.get(i);
            out(String.format("%-30s %10d ms", "Repair PerCall time" , time));

            time = eqStats.get(i);
            out(String.format("%-30s %10d ms", "EQ time", time));

            long nCell = cellStats.get(i);
            out(String.format("%-30s %10d", "Cell updated", nCell));
            totalChangedCell += nCell;
            out("----------------------------------------------------------------");
        }
        System.out.println(
            "Repair " + repairStats.size() + " rules finished in " + totalTime + " ms " +
            "with " + totalChangedCell + " cells changed.\n"
        );
    }

    public static void printDetectSummary() {
        out("Detection summary:");

        long totalTime = 0;
        int totalViolation = 0;
        List<Long> detectStats = Lists.newArrayList(stats.get(StatType.DetectTime));
        List<Long> violationExport = Lists.newArrayList(stats.get(StatType.ViolationExport));

        for (int i = 0; i < detectStats.size(); i ++) {
            out("Rule " + i + " :");
            out("----------------------------------------------------------------");
            out(formatEntry(StatType.HScopeTime, "HScope time", "ms", i));
            out(formatEntry(StatType.VScopeTime, "VScope time", "ms", i));
            out(formatEntry(StatType.Blocks, "Blocks", "", i));
            out(formatEntry(StatType.IterationCount, "Original tuple count", "", i));
            out(formatEntry(StatType.IteratorTime, "Iterator time", "ms", i));
            out(formatEntry(StatType.DBLoadTime, "DB load time", "ms", i));
            out(formatEntry(StatType.DetectTime, "Detect time", "ms", i));
            out(formatEntry(StatType.DetectCallTime, "Detect call time", "ms", i));
            out(formatEntry(StatType.DetectThreadCount, "Detect thread count", "", i));
            out(formatEntry(StatType.DetectCount, "Detect tuple count", "", i));
            out(formatEntry(StatType.ViolationExport, "Violation", "", i));
            out(formatEntry(StatType.ViolationExportTime, "Violation export time", "", i));
            long time = detectStats.get(i);
            totalTime += time;

            long num = violationExport.get(i);
            totalViolation += num;
        }
        out("----------------------------------------------------------------");

        System.out.println(
            "Detection " + detectStats.size() + " rules finished in " + totalTime + " ms " +
            "and found " + totalViolation + " violations.\n"
        );
    }

    private static String formatEntry(
        StatType type,
        String prefix,
        String suffix,
        int index
    ) {
        List<Long> statValue = Lists.newArrayList(stats.get(type));
        long value;
        if (statValue.size() > index) {
            value = statValue.get(index);
        } else {
            value = 0;
        }
        return String.format("%-30s %10d %s", prefix, value, suffix);
    }

    private static void out(String msg) {
        if (infoFlag) {
            System.out.println(msg);
        }
    }
    //</editor-fold>
}
