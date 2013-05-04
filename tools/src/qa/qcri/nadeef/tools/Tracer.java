/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * Tracer is used for debugging / profiling / benchmarking purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private static Multimap<String, String> stats = ArrayListMultimap.create();
    private Class classType;
    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;
    private static boolean statFlag = true;

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
            System.out.println(":INFO:In " + classType.getSimpleName() + " : " + msg);
        }
    }

    public void verbose(String msg) {
        if (verboseFlag) {
            System.out.println(":VERBOSE:In " + classType.getName() + " : " + msg);
        }
    }

    public void err(String msg) {
        System.err.println("In " + classType.getName() + " : " + msg);
    }

    public static void addStatEntry(String key, String value) {
        stats.put(key, value);
    }
    //</editor-fold>

    //<editor-fold desc="Static methods">
    public static void setInfo(boolean mode) {
        infoFlag = mode;
    }

    public static void setStats(boolean mode) {
        statFlag = mode;
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

    public static boolean isStatsOn() {
        return statFlag;
    }

    public static void printStats() {
        StringBuilder output = new StringBuilder();
        Set<String> keys = stats.keySet();
        for (String key : keys) {
            Collection<String> values = stats.get(key);
            int i = 0;
            for (String value : values) {
                if (i == 0) {
                    System.out.println(String.format("%-30.30s %-30.30s%n", key, value));
                } else {
                    System.out.println(String.format("%-30.30s %-30.30s%n", "", value));
                }
                i ++;
            }
        }
    }
    //</editor-fold>
}
