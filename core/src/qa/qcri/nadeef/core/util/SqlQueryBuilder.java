/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * SQL Builder utility.
 */
public class SqlQueryBuilder {
    private List<String> selects;
    private List<String> wheres;
    private List<String> froms;
    private List<String> orders;
    private boolean isDistinct;

    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     */
    public SqlQueryBuilder() {
        selects = new ArrayList();
        wheres = new ArrayList();
        froms = new ArrayList();
        orders = new ArrayList();
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public void addSelect(String select) {
        Preconditions.checkNotNull(selects);
        this.selects.add(select);
    }

    public void addSelect(List<String> selects) {
        Preconditions.checkNotNull(selects);
        this.selects.addAll(selects);
    }

    public void addOrder(List<String> orders) {
        Preconditions.checkNotNull(orders);
        this.orders.addAll(orders);
    }

    public void addOrder(String order) {
        Preconditions.checkNotNull(order);
        this.orders.add(order);
    }

    public void addWhere(String where) {
        Preconditions.checkNotNull(where);
        this.wheres.add(where);
    }

    public void addWhere(List<String> wheres) {
        Preconditions.checkNotNull(wheres);
        this.wheres.addAll(wheres);
    }

    public void addFrom(String from) {
        Preconditions.checkNotNull(from);
        this.froms.add(from);
    }

    public void addFrom(List<String> froms) {
        Preconditions.checkNotNull(froms);
        this.froms.addAll(froms);
    }

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public String toSQLString() {
        StringBuilder builder = new StringBuilder("SELECT ");
        if (isDistinct) {
            builder.append(" DISTINCT ");
        }

        builder.append(asString(selects, "*"));
        builder.append(" FROM ");
        builder.append(asString(froms));
        builder.append(" WHERE ");
        builder.append(asString(wheres));
        return builder.toString();
    }
    //</editor-fold>

    //<editor-fold desc="Private methods">
    private StringBuilder asString(Collection<String> list, String defaultString) {
        if (list.size() == 0) {
            return new StringBuilder(defaultString);
        }
        return asString(Arrays.asList(defaultString));
    }

    private StringBuilder asString(Collection<String> list) {
        StringBuilder builder = new StringBuilder();
        String[] attrs = list.toArray(new String[list.size()]);
        int i = 0;
        for (String attr : attrs) {
            if (i != 0) {
                builder.append(',');
            }
            builder.append(attr);
        }
        return builder;
    }
    //</editor-fold>
}
