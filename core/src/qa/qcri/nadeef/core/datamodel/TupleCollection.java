/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Tuple collection class.
 */
public class TupleCollection extends ArrayList<Tuple> {
    public TupleCollection(Collection<Tuple> collection) {
        super(collection);
    }
}
