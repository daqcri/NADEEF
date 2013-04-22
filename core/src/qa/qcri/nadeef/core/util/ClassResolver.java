/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

/**
 * Class resolving in runtime.
 */
public class ClassResolver {

    /**
     * Loads a class in runtime.
     * @param className class name.
     * @return Class type.
     */
    public static Class loadClass(String className) throws ClassNotFoundException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        return classLoader.loadClass(className);
    }
}
