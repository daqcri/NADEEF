/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Common file operations to solve platform-independent issue.
 */
public class FileHelper {
    private static boolean isWindows() {
        String osName = System.getProperty("os.name");
        return osName.indexOf("Win") >= 0;
    }

    private static boolean isMac() {
        String osName = System.getProperty("os.name");
        return osName.indexOf("mac") >= 0;
    }

    /**
     * Gets the file object given the full file name.
     * @param fileName full file name.
     * @return File object.
     */
    public static File getFile(String fileName) throws FileNotFoundException {
        if (isWindows()) {
            fileName = fileName.replace("\\", "\\\\");
        } else {
            fileName = fileName.replace("\\", "/");
        }

        File file = new File(fileName);
        if (!file.exists()) {
            throw new FileNotFoundException();
        }
        if (!file.isFile()) {
            throw new IllegalArgumentException("The given filename is not a file.");
        }
        return file;
    }
}
