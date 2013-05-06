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
        List<String> fixStats = Lists.newArrayList(stats.get(StatType.FixDeserialize));
        List<String> cellStats = Lists.newArrayList(stats.get(StatType.UpdatedCellNumber));
        long totalTime = 0;
        int totalFix = 0;
        int totalChangedCell = 0;

        for (int i = 0; i < detectStats.size(); i ++) {
            long time = Long.parseLong(detectStats.get(i));
            out(String.format("%-30s %10d ms", "Repair Rule " + i, time));
            time = Long.parseLong(eqStats.get(i));
            totalTime += time;
            out(String.format("%-30s %10d ms", "EQ Rule " + i, time));
            totalTime += time;

            int nFix = Integer.parseInt(fixStats.get(i));
            out(String.format("%-30s %10d", "Rule " + i + " has candidate fixes", nFix));
            totalFix += nFix;

            int nCell = Integer.parseInt(cellStats.get(i));
            out(String.format("%-30s %10d", "Rule " + i + " has Cell updated", nCell));
            totalChangedCell += nCell;
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

        for (int i = 0; i < detectStats.size(); i ++) {
            long time = Long.parseLong(detectStats.get(i));
            out(String.format("%-30s %10d ms", "Rule " + i, time));
            totalTime += time;

            int num = Integer.parseInt(violationExport.get(i));
            out(String.format("%-30s %10d", "Rule " + i + " found violation", num));
            totalViolation += num;
        }

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
