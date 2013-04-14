package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;

/**
 * Tuple Pair entity.
 */
public class TuplePair {

    private Tuple left;
    private Tuple right;

    public TuplePair(Tuple left, Tuple right) {
        Preconditions.checkArgument(left != null && right != null, "Pair cannot have null values.");
        this.left = left;
        this.right = right;
    }

    //<editor-fold desc="Getters">
    public Tuple getLeft() {
        return left;
    }

    public Tuple getRight() {
        return right;
    }
    //</editor-fold>
}
