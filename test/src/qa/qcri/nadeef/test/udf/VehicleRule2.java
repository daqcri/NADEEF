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

package qa.qcri.nadeef.test.udf;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class VehicleRule2 extends PairTupleRule {
    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    /**
     * Detect method.
     * @param tuplePair tuple pair.
     * @return violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        List<Violation> result = new ArrayList<>();
        Tuple left = tuplePair.getLeft();
        Tuple right = tuplePair.getRight();

        String model1 = getValue(tuplePair, "vehicles_copy", "model", 0);
        String model2 = getValue(tuplePair, "qatarsales_vehicles_copy", "model", 1);
        String contact1 = getValue(tuplePair, "vehicles_copy", "contact_number", 0);
        String contact2 = getValue(tuplePair, "qatarsales_vehicles_copy", "contact_number", 1);
        String brandId1 = getValue(tuplePair, "vehicles_copy", "brand_id", 0);
        String brandId2 = getValue(tuplePair, "qatarsales_vehicles_copy", "brand_id", 1);
        String price1s = getValue(tuplePair, "vehicles_copy", "price", 0);
        String price2s = getValue(tuplePair, "qatarsales_vehicles_copy", "price", 1);

        if (
            Metrics.getLevenshtein(model1, model2) == 1.0 &&
            Metrics.getLevenshtein(contact1, contact2) > 0.8 &&
            Metrics.getLevenshtein(brandId1, brandId2) == 1.0 &&
            withInPriceRange(price1s, price2s)
        ) {
            Violation violation = new Violation(getRuleName());
            violation.addCell(left.getCell("id"));
            violation.addCell(right.getCell("id"));
            result.add(violation);
        }

        return result;
    }

    private boolean withInPriceRange(String p1, String p2) {
        if (Strings.isNullOrEmpty(p1) || Strings.isNullOrEmpty(p2)) {
            return true;
        }

        Double price1 = str2double(p1);
        Double price2 = str2double(p2);
        return Math.abs(price1 - price2) * 2.0 / (price2 + price1) < 0.2;
    }

    private Double str2double(String s) {
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < s.length(); i ++) {
            char c = s.charAt(i);
            if (c >= '0' && c <='9')
                buf.append(c);
        }

        double result = 0;
        try {
            result = Double.parseDouble(buf.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(buf.toString());
        }
        return result;
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = Lists.newArrayList();
        List<Cell> cells = Lists.newArrayList(violation.getCells());

        Fix.Builder fixBuilder = new Fix.Builder(violation);
        Fix fix = fixBuilder.left(cells.get(0)).right(cells.get(1)).build();
        result.add(fix);
        return result;
    }

    private String getValue(TuplePair pair, String tableName, String column, int isLeft) {
        Tuple left = pair.getLeft();
        Tuple right = pair.getRight();
        String result;
        if (isLeft == 0) {
            if (left.isFromTable(tableName)) {
                Object obj = left.get(column);
                result = obj != null ? obj.toString() : null;
            } else {
                Object obj = right.get(column);
                result = obj != null ? obj.toString() : null;
            }
        } else {
            if (right.isFromTable(tableName)) {
                Object obj = right.get(column);
                result = obj != null ? obj.toString() : null;
            } else {
                Object obj = left.get(column);
                result = obj != null ? obj.toString() : null;
            }
        }
        return result;
    }
}