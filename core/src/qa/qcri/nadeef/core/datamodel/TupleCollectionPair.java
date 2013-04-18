/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;

/**
 * Tuple collection pair.
 */
public class TupleCollectionPair {
    private TupleCollection left;
    private TupleCollection right;

    public TupleCollectionPair(TupleCollection left, TupleCollection right) {
        Preconditions.checkNotNull(left);
        Preconditions.checkNotNull(right);
        this.left = left;
        this.right = right;
    }

    public TupleCollection getLeft() {
        return left;
    }

    public TupleCollection getRight() {
        return right;
    }
}
