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
    private static boolean timingFlag = true;
    private static boolean infoFlag = true;

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
            System.out.println("In " + classType.getName() + " : " + msg);
        }
    }

    public void err(String msg) {
        System.err.println("In " + classType.getName() + " : " + msg);
    }

    /**
     * Timing information.
     * @param msg message.
     * @param milliseconds elapsed time.
     */
    public void timing(String msg, long milliseconds) {
        if (timingFlag) {
            System.out.println("::" + msg + " : " + milliseconds);
        }
    }

    public static void setInfo(boolean mode) {
        infoFlag = mode;
    }

    //</editor-fold>

}
