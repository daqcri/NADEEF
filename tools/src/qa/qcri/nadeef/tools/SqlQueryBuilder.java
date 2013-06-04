/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SQL Builder utility.
 */
public class SqlQueryBuilder implements Cloneable {
    private Set<String> selects;
    private Set<String> wheres;
    private Set<String> froms;
    private Set<String> orders;
    private Set<String> distincts;
    private int limit;

    //<editor-fold desc="Constructor">

    /**
     * Constructor.
     */
    public SqlQueryBuilder() {
        selects = Sets.newHashSet();
        wheres = Sets.newHashSet();
        froms = Sets.newHashSet();
        orders = Sets.newHashSet();
        distincts = Sets.newHashSet();
        limit = -1;
    }

    /**
     * Copy Constructor.
     */
    public SqlQueryBuilder(SqlQueryBuilder obj) {
        selects = new HashSet<String>(obj.selects);
        wheres = new HashSet<String>(obj.wheres);
        froms = new HashSet<String>(obj.froms);
        orders = new HashSet<String>(obj.orders);
        distincts = new HashSet<String>(obj.distincts);
        limit = obj.limit;
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

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String build() {
        StringBuilder builder = new StringBuilder("SELECT ");
        if (distincts.size() > 0) {
            builder.append(" DISTINCT ON (");
            builder.append(asString(distincts, ","));
            builder.append(") ");
        }

        if (selects.size() != 0 && !selects.contains("tid")) {
            selects.add("tid");
        }

        if (selects.size() == 0) {
            builder.append("*");
        } else {
            builder.append(asString(selects, ","));
        }

        builder.append(" FROM ");

        builder.append(asString(froms, ","));

        if (wheres.size() > 0) {
            builder.append(" WHERE ");
            builder.append(asString(wheres, " AND "));
        }

        if (orders.size() > 0) {
            builder.append(" ORDER BY ");
            builder.append(asString(orders, ","));
        }

        if (limit > 0) {
            builder.append(" LIMIT ");
            builder.append(limit);
        }
        return builder.toString();
    }
    //</editor-fold>

    //<editor-fold desc="Private methods">
    private String asString(Collection<String> list, String separator) {
        Preconditions.checkArgument(list != null && list.size() > 0);
        return Joiner.on(separator).skipNulls().join(list);
    }

    //</editor-fold>
}
