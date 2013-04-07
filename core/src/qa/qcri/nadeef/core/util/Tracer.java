/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

/**
 * Tracer is used for debugging / profiling / diagnoising purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private static final Tracer instance = new Tracer();
    private Tracer() {}

    public static Tracer getInstance() {
        return instance;
    }

    public void info(String msg) {
        System.out.println(msg);
    }

    public void err(String msg) {
        System.err.println(msg);
    }
}
