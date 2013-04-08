/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import qa.qcri.nadeef.core.datamodel.Primitive;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Tuple class.
 * TODO: consider using Trove for better hashmap performance.
 * TODO: use better index instead of string
 */
public class Tuple extends Primitive {

    //<editor-fold desc="Private Fields">
    private HashMap<String, Object> dict;
    private String tableName;

    //</editor-fold>

    //<editor-fold desc="Public Members">

    /**
     * Constructor.
     */
    public Tuple(String tableName, String[] attributeNames, Object[] values) {
        this.tableName = tableName;

        if (attributeNames == null || values == null) {
            throw new IllegalArgumentException("Input attribute/value cannot be null.");
        }
        if (attributeNames.length != values.length) {
            throw new IllegalArgumentException("Incorrect input with attributes and values");
        }

        dict = new HashMap(attributeNames.length);
        for (int i = 0; i < attributeNames.length; i ++) {
            dict.put(attributeNames[i], values[i]);
        }
    }

    /**
     * Gets the value from the tuple.
     * @param key The attribute key
     * @return Output Value
     */
    public Object get(String key) {
        return dict.get(key);
    }

    /**
     * Gets all the values in the tuple.
     * @return value collections.
     */
    public Collection<Object> getValues() {
        return dict.values();
    }

    /**
     * Check whether the tuple has an attribute.
     * @param attributeName Attribute name.
     * @return true when the attribute exists.
     */
    public boolean hasAttribtue(String attributeName) {
        return dict.containsKey(attributeName);
    }
    //</editor-fold>

}
