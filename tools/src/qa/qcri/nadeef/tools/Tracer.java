/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.collect.*;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Tracer is used for debugging / profiling / benchmarking purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private static Multimap<StatType, String> stats = ArrayListMultimap.create();
    private Class classType;
    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;

    public enum StatType {
        // detect
        DetectTime,
        ViolationExport,
        ViolationExportTime,
        TupleNumber,
        // repair
        RepairTime,
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

    public void err(String msg) {
        System.err.println(":ERROR:" + classType.getName() + " : " + msg);
    }

    public static void addStatEntry(StatType statType, String value) {
        stats.put(statType, value);
    }

    //</editor-fold>

    //<editor-fold desc="Static methods">
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
        List<String> detectStats = Lists.newArrayList(stats.get(StatType.RepairTime));
        List<String> eqStats = Lists.newArrayList(stats.get(StatType.EQTime));
        List<String> cellStats = Lists.newArrayList(stats.get(StatType.UpdatedCellNumber));

        long totalTime = 0;
        int totalChangedCell = 0;

        for (int i = 0; i < detectStats.size(); i ++) {
            long time = Long.parseLong(detectStats.get(i));
            out("Rule " + i + ":");
            out("----------------------------------------------------------------");
            out(String.format("%-30s %10d ms", "Repair time" , time));
            totalTime += time;

            time = Long.parseLong(eqStats.get(i));
            out(String.format("%-30s %10d ms", "EQ time", time));
            totalTime += time;

            int nCell = Integer.parseInt(cellStats.get(i));
            out(String.format("%-30s %10d", "Cell updated", nCell));
            totalChangedCell += nCell;
            out("----------------------------------------------------------------");
        }
        System.out.println(
            "Repair " + detectStats.size() + " rules finished in " + totalTime + " ms " +
            "with " + totalChangedCell + " cells changed.\n"
        );
    }

    public static void printDetectSummary() {
        out("Detection summary:");

        long totalTime = 0;
        int totalViolation = 0;
        List<String> detectStats = Lists.newArrayList(stats.get(StatType.DetectTime));
        List<String> violationExport = Lists.newArrayList(stats.get(StatType.ViolationExport));
        List<String> violationExportTime =
            Lists.newArrayList(stats.get(StatType.ViolationExportTime));
        List<String> tupleNums = Lists.newArrayList(stats.get(StatType.TupleNumber));

        for (int i = 0; i < detectStats.size(); i ++) {
            out("Rule " + i + " :");
            out("----------------------------------------------------------------");
            long time = Long.parseLong(detectStats.get(i));
            out(String.format("%-30s %10d ms", "Detect time", time));
            totalTime += time;

            int nTuple = Integer.parseInt(tupleNums.get(i));
            out(String.format("%-30s %10d", "Detect tuple", nTuple));

            int num = Integer.parseInt(violationExport.get(i));
            out(String.format("%-30s %10d", "Violation", num));
            totalViolation += num;

            long exportTime = Long.parseLong(violationExportTime.get(i));
            out(String.format("%-30s %10d ms", "Violation export time", exportTime));
        }
        out("----------------------------------------------------------------");

        System.out.println(
            "Detection " + detectStats.size() + " rules finished in " + totalTime + " ms " +
            "and found " + totalViolation + " violations.\n"
        );
    }

    private static void out(String msg) {
        if (infoFlag) {
            System.out.println(msg);
        }
    }


    //</editor-fold>
}
