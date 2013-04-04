/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import qa.qcri.nadeef.core.datamodel.Primitive;

import java.util.Collection;
import java.util.HashMap;

/**
 * Tuple class.
 */
public class Tuple extends Primitive {

    //<editor-fold desc="Private Fields">
    private HashMap<String, Object> dict;
    //</editor-fold>

    //<editor-fold desc="Public Members">

    /**
     * Constructor.
     */
    public Tuple() {
        dict = new HashMap(0);
    }

    /**
     * Gets the value from the tuple.
     * @param key The attribute key
     * @return Output Value
     */
    public Object getValue(String key) {
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
