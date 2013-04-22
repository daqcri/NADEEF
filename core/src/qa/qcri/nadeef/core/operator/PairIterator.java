/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.core.datamodel.TuplePair;

import java.util.Collection;

/**
 * Pair Iterator class.
 */
public abstract class PairIterator
    extends Operator<Collection<TupleCollection>, Collection<TuplePair>> {
}
