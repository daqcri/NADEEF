package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;

/**
 * Tuple Pair represents a pair of tuples.
 */
public class TuplePair {
    private Tuple left;
    private Tuple right;

    /**
     * Constructor.
     * @param left left tuple.
     * @param right right tuple.
     */
    public TuplePair(Tuple left, Tuple right) {
        Preconditions.checkArgument(
            left != null && right != null, "Pair cannot have null values."
        );
        this.left = left;
        this.right = right;
    }

    /**
     * Private constructor.
     */
    TuplePair() {}

    //<editor-fold desc="Getters">

    /**
     * Get the left tuple.
     * @return the left tuple.
     */
    public Tuple getLeft() {
        return left;
    }

    /**
     * Gets the right tuple.
     * @return the right tuple.
     */
    public Tuple getRight() {
        return right;
    }

    //</editor-fold>
}
