/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.tools.*;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

/**
 * Common helper tools
 */
public class CommonTools {

    /**
     * Loads a class in runtime using specified classpath.
     * @param className class name.
     * @param url classpath url.
     * @return Class type.
     */
    public static Class loadClass(String className, URL url) throws ClassNotFoundException {
        URL[] urls = new URL[] { url };
        URLClassLoader ucl = new URLClassLoader(urls);
        return ucl.loadClass(className);
    }

    /**
     * Loads a class in runtime using default classpath.
     * @param className class name.
     * @return Class type.
     */
    public static Class loadClass(String className) throws ClassNotFoundException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        return classLoader.loadClass(className);
    }

    /**
     * Returns <code>True</code> when the given string is a numerical string.
     * @param string input string.
     * @return <code>True</code> when the given string is a numerical string.
     */
    public static boolean isNumericalString(String string) {
        return string.matches("-?\\d+(\\.\\d+)?");
    }

    /**
     * Returns <code>True</code> when the given attribute name is a valid full column name.
     * @param attributeName Column name.
     * @return <code>True</code> when the column name is valid.
     */
    public static boolean isValidColumnName(String attributeName) {
        String[] tokens = attributeName.split("\\.");
        // TODO: change to correct regexp.
        if (
            tokens.length != 2 ||
            Strings.isNullOrEmpty(tokens[0]) ||
            Strings.isNullOrEmpty(tokens[1])
        ) {
            return false;
        }
        return true;
    }

    /**
     * Compiles the input file and generates the output class file.
     * @param fullFilePath input .java file path.
     * @return class file path.
     */
    public static boolean compileFile(File fullFilePath) throws IOException {
        Preconditions.checkArgument(fullFilePath != null && fullFilePath.isFile());

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics =
            new DiagnosticCollector<JavaFileObject>();
        StandardJavaFileManager fileManager =
            compiler.getStandardFileManager(diagnostics, null, null);
        Iterable<? extends JavaFileObject> compilationUnit =
            fileManager.getJavaFileObjectsFromFiles(Arrays.asList(fullFilePath));
        List<String> args = Arrays.asList("-Xlint:none");
        boolean result =
            compiler.getTask(null, fileManager, diagnostics, args, null, compilationUnit).call();

        if (!result) {
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                System.out.format(
                    "Error on line %d in %s%n",
                    diagnostic.getLineNumber(),
                    diagnostic.getMessage(null)
                );
            }
        }
        fileManager.close();
        return result;
    }
}
