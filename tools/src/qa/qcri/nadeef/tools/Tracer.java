/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

/**
 * Tracer is used for debugging / profiling / benchmarking purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private Class classType;
    private static boolean infoFlag = true;
    private static boolean verboseFlag = false;

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
            System.out.println(msg);
        }
    }

    public void verbose(String msg) {
        if (verboseFlag) {
            System.out.println("In " + classType.getName() + " : " + msg);
        }
    }

    public void err(String msg) {
        System.err.println("In " + classType.getName() + " : " + msg);
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
    //</editor-fold>

}
