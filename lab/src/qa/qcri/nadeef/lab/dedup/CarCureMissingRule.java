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

package qa.qcri.nadeef.lab.dedup;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.CSVTools;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class CarCureMissingRule extends SingleTupleRule {
    private static HashSet<String> brandDict;
    static {
        try {
            List<String[]> data = CSVTools.read(
                new File("lab/src/qa/qcri/nadeef/lab/dedup/brands.csv"),
                ","
            );

            brandDict = Sets.newHashSet();

            for (String[] entry : data) {
                brandDict.add(entry[0].trim().toLowerCase());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Collection<Violation> detect(Tuple tuple) {
        String title = (String)tuple.get("title");
        if (title == null)
            return null;

        Integer model = (Integer)tuple.get("model");
        String brand_name = (String)tuple.get("brand_name");

        if (model != null && brand_name != null)
            return null;

        List<Violation> result = Lists.newArrayList();
        Violation violation = new Violation(this.getRuleName());
        violation.addCell(tuple.getCell("title"));
        if (model == null) {
            violation.addCell(tuple.getCell("model"));
        }

        if (brand_name == null) {
            violation.addCell(tuple.getCell("brand_name"));
        }


        result.add(violation);
        return result;
    }

    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Cell> cells = Lists.newArrayList(violation.getCells());
        String title = null;
        Cell model = null;
        Cell brand = null;
        boolean isBrandMissing = false;
        boolean isModelMissing = false;

        for (Cell cell : cells) {
            if (cell.hasColumnName("title")) {
                title = cell.getValue();
            }

            if (cell.hasColumnName("model")) {
                model = cell;
                isModelMissing = true;
            }

            if (cell.hasColumnName("brand_name")) {
                brand = cell;
                isBrandMissing = true;
            }
        }

        String[] tokens = title.split("[\\s,.]");
        Fix.Builder builder = new Fix.Builder(violation);
        List<Fix> fixes = Lists.newArrayList();
        Integer modelValue = null;
        String brandValue = null;
        for (String token : tokens) {
            if (isModelMissing) {
                modelValue = getModel(token);
                if (modelValue != null) {
                    Fix fix =
                        builder
                            .left(model)
                            .op(Operation.EQ)
                            .right(modelValue.toString())
                            .build();
                    fixes.add(fix);
                    isModelMissing = false;
                }
            }

            if (isBrandMissing) {
                brandValue = getBrand(token);
                if (brandValue != null) {
                    Fix fix =
                        builder
                            .left(brand)
                            .op(Operation.EQ)
                            .right(brandValue)
                            .build();
                    fixes.add(fix);
                    isBrandMissing = false;
                }
            }

            if (!isBrandMissing && !isModelMissing) {
                System.out.println(
                    "Got brand [" + brandValue + "] model [" + modelValue + "] from " + title
                );
                break;
            }
        }

        return fixes;
    }

    private Double getPrice(String s) {
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < s.length(); i ++) {
            char c = s.charAt(i);
            if (c >= '0' && c <='9')
                buf.append(c);
        }

        double result = 0;
        try {
            result = Double.parseDouble(buf.toString());
        } catch (Exception ex) {
            System.out.println("Unknown price data: " + s);
        }
        return result;
    }

    private Integer getModel(String token) {
        StringBuilder builder = new StringBuilder();

        for (char c : token.toCharArray()) {
            if (c >= '0' && c <= '9')
                builder.append(c);
            else if (builder.length() != 0) {
                int v = 0;
                try {
                    v = Integer.parseInt(builder.toString());
                } catch (NumberFormatException ex) {
                    System.out.println(ex.getMessage());
                }
                if (v < 2020 && v > 1900)
                    return v;
                builder = new StringBuilder();
            }
        }

        if (builder.length() != 0) {
            int v = 0;
            try {
                v = Integer.parseInt(builder.toString());
            } catch (NumberFormatException ex) {
                System.out.println(ex.getMessage());
            }
            if (v < 2020 && v > 1900)
                return v;
        }
        return null;
    }

    private String getBrand(String token) {
        token = token.toLowerCase();
        if (brandDict.contains(token)) {
            return token;
        }
        return null;
    }
}
