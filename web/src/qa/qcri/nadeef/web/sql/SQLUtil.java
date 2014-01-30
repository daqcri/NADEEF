/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.web.sql;

import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtil {
    public static boolean isValidTableName(String s) {
        boolean isGood = true;
        if (!Strings.isNullOrEmpty(s)) {
            Pattern pattern = Pattern.compile("\\w+");
            Matcher matcher = pattern.matcher(s);
            if (!matcher.find())
                isGood = false;
        }
        return isGood;
    }

    public static boolean isValidInteger(String s) {
        boolean isGood = true;
        if (!Strings.isNullOrEmpty(s)) {
            try {
                int ignore = Integer.parseInt(s);
            } catch (Exception ex) {
                isGood = false;
            }

        }
        return isGood;
    }
}
