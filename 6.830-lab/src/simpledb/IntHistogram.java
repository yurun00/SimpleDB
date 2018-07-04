package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] buckets;
    private double width;
    private int min;
    private int max;
    private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        // All intervals are left-closed and right open.
        this.buckets = new int[Math.min(buckets, max-min+1)];
        this.min = min;
        this.max = max;
        width = (max-min+1) * 1.0 / this.buckets.length;
        ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) {
            throw new IllegalArgumentException(String.format("value %d out of bound [%d, %d]", v, min, max));
        }
        int idx = (int)((v-min) / width);
        buckets[idx]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int idx = (int)((v-min) / width);
        if (op == Predicate.Op.EQUALS) {
            if (v < min || v > max)
                return 0;
            int h = buckets[idx];
            return h / width / ntups;
        }
        else if (op == Predicate.Op.GREATER_THAN) {
            if (v >= max)
                return 0;
            if (v < min)
                return 1;
            int h = buckets[idx];
            double cnt = h * (Math.ceil(min + (idx+1) * width -1) - v) / width;
            for (int i = idx+1;i < buckets.length;i++) {
                cnt += buckets[i];
            }
            cnt /= ntups;
            return cnt;
        }
        else if (op == Predicate.Op.LESS_THAN) {
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v) 
                - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        }
        else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }
        else if (op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistogram, buckets: %d, min: %d, max: %d, width: %f, ntups: %d."
            , buckets.length, min, max, width, ntups);
    }
}
