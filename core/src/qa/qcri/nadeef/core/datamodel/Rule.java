/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created with IntelliJ IDEA.
 * User: si
 * Date: 4/5/13
 * Time: 9:45 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Rule {
    public Violation detect(Tuple tuple) {
        throw new NotImplementedException();
    }

    public Violation detect(Tuple tuple1, Tuple tuple2) {
        throw new NotImplementedException();
    }

    public Violation detect(Iterable<Tuple> tupleIterator) {
        throw new NotImplementedException();
    }
}
