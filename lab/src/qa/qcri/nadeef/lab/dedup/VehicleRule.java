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
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class VehicleRule extends PairTupleRule {
    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }


    /**
     * Default block operation.
     * @param tables a collection of tables.
     * @return a collection of blocked tables.
     */
    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = tables.iterator().next();
        Collection<Table> groupResult = table.groupOn("model");
        return groupResult;
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

        Object contact1 = left.get("contact_number");
        Object contact2 = right.get("contact_number");
        Object brandId1 = left.get("brand_id");
        Object brandId2 = right.get("brand_id");
        Object price1s = left.get("price");
        Object price2s = right.get("price");

        if (
            similarContactNumber(contact1, contact2) &&
            sameBrand(brandId1, brandId2) &&
            withInPriceRange(price1s, price2s)
        ) {
            Violation violation = new Violation(getRuleName());
            violation.addCell(left.getCell("tid"));
            violation.addCell(right.getCell("tid"));
            result.add(violation);
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

    private boolean sameBrand(Object a, Object b) {
        if (a == null || b == null)
            return true;
        return a.equals(b);
    }

    private boolean similarContactNumber(Object a, Object b) {
        if (a == null || b == null)
            return true;
        String as = (String)a;
        String bs = (String)b;
        return Metrics.getLevenshtein(as, bs) > 0.8;
    }

    private boolean withInPriceRange(Object a, Object b) {
        if (a == null || b == null)
            return true;

        Double price1 = str2double((String)a);
        Double price2 = str2double((String)b);
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

}