/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

/**
 * Common helper tools
 */
public class Tools {

    /**
     * Returns <code>True</code> when the given string is a numerical string.
     * @param string input string.
     * @return <code>True</code> when the given string is a numerical string.
     */
    public static boolean isNumericalString(String string) {
        return string.matches("-?\\d+(\\.\\d+)?");
    }
}
