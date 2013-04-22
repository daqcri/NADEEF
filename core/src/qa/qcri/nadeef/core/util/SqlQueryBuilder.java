/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SQL Builder utility.
 */
public class SqlQueryBuilder {
    private Set<String> selects;
    private Set<String> wheres;
    private Set<String> froms;
    private Set<String> orders;
    private Set<String> distincts;

    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     */
    public SqlQueryBuilder() {
        selects = new HashSet();
        wheres = new HashSet();
        froms = new HashSet();
        orders = new HashSet();
        distincts = new HashSet();
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

    public void addDistinct(Collection<String> distincts) {
        this.distincts.addAll(distincts);
    }

    public void addDistinct(String disintct) {
        this.distincts.add(disintct);
    }

    public String toSQLString() {
        StringBuilder builder = new StringBuilder("SELECT ");
        if (distincts.size() > 0) {
            builder.append(" DISTINCT ON (");
            builder.append(asString(distincts));
            builder.append(") ");
        }

        builder.append(asString(selects, "*"));
        builder.append(" FROM ");
        builder.append(asString(froms));
        if (wheres.size() > 0) {
            builder.append(" WHERE ");
            builder.append(asString(wheres));
        }

        if (orders.size() > 0) {
            builder.append(" ORDER BY ");
            builder.append(asString(orders));
        }

        return builder.toString();
    }
    //</editor-fold>

    //<editor-fold desc="Private methods">
    private String asString(Collection<String> list, String defaultString) {
        if (list.size() == 0) {
            return defaultString;
        }
        return asString(list);
    }

    private String asString(Collection<String> list) {
        return Joiner.on(',').skipNulls().join(list);
    }
    //</editor-fold>
}
