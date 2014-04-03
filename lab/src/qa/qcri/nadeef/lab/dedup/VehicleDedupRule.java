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
import com.google.common.collect.Maps;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class VehicleDedupRule extends PairTupleRule {

    private static HashMap<Integer, Integer> gid = Maps.newHashMap();
    private static int maxGid = 0;

    private synchronized boolean hasTid(int vid) {
        return gid.containsKey(vid);
    }

    private synchronized int getGid(int vid) {
        return gid.get(vid);
    }

    private synchronized void putGid(int vid, int nid) {
        gid.put(vid, nid);
    }

    private synchronized int putGid(int vid) {
        gid.put(vid, maxGid);
        return maxGid ++;
    }

    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    /*
    @Override
    public Collection<Table> horizontalScope(Collection<Table> tables) {
        Table table = tables.iterator().next();
        String tableName = table.getSchema().getTableName();
        List<Predicate> predicates = Lists.newArrayList();

        predicates.add(
            new Predicate.PredicateBuilder()
                .left(new Column(tableName, "brand_id"))
                .op(Operation.NEQ)
                .constant(null)
                .build()
        );

        predicates.add(
            new Predicate.PredicateBuilder()
                .left(new Column(tableName, "title"))
                .op(Operation.NEQ)
                .constant(null)
                .build()
        );

        List<Table> result = Lists.newArrayList();
        result.add(table.filter(predicates));
        return result;
    }
    */

    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = tables.iterator().next();
        return table.groupOn("brand_name");
    }

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
        Object model1 = left.get("model");
        Object model2 = right.get("model");
        int id1 = (Integer)left.get("id");
        int id2 = (Integer)right.get("id");

        if (
            similarContactNumber(contact1, contact2) &&
            sameBrand(brandId1, brandId2) &&
            sameModel(model1, model2) &&
            withInPriceRange(price1s, price2s)
        ) {
            Violation violation = new Violation(getRuleName());
            int lgroup = (Integer)left.get("duplicate_group");
            int rgroup = (Integer)right.get("duplicate_group");
            violation.addCell(left.getCell("duplicate_group"));
            violation.addCell(right.getCell("duplicate_group"));
            result.add(violation);
        }

        return result;
    }

    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = Lists.newArrayList();
        List<Cell> cells = Lists.newArrayList(violation.getCells());

        Fix.Builder fixBuilder = new Fix.Builder(violation);
        Fix fix = fixBuilder.left(cells.get(0)).right(cells.get(1)).build();
        result.add(fix);
        return result;
    }

    private boolean sameModel(Object a, Object b) {
        if (a == null && b == null)
            return true;

        if (a == null && b != null)
            return false;

        if (a != null && b == null)
            return false;

        int am = (Integer)a;
        int bm = (Integer)b;
        return am == bm;
    }

    private boolean sameBrand(Object a, Object b) {
        if (a == null && b == null)
            return true;

        if (a == null && b != null)
            return false;

        if (a != null && b == null)
            return false;

        return a.equals(b);
    }

    private boolean similarContactNumber(Object a, Object b) {
        String as = (String)a;
        String bs = (String)b;

        boolean isNE1 = Strings.isNullOrEmpty(as);
        boolean isNE2 = Strings.isNullOrEmpty(bs);

        if (isNE1 && isNE2) {
            return true;
        }

        if (isNE1 || isNE2) {
            return false;
        }

        return Metrics.getLevenshtein(as, bs) > 0.8;
    }

    private boolean withInPriceRange(Object a, Object b) {
        if (a == null || b == null)
            return true;

        String price1s = (String)a;
        String price2s = (String)b;

        boolean isNE1 = Strings.isNullOrEmpty(price1s);
        boolean isNE2 = Strings.isNullOrEmpty(price2s);

        if (isNE1 && isNE2) {
            return true;
        }

        if (isNE1 || isNE2) {
            return false;
        }

        Double price1 = str2double(price1s);
        Double price2 = str2double(price2s);
        return Math.abs(price1 - price2) * 2.0 / (price2 + price1) < 0.2;
    }

    private Double str2double(String s) {
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
            // System.out.println("Unknown price data: " + s);
        }
        return result;
    }
}
