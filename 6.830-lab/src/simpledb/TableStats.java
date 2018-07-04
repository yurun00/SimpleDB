package simpledb;
import java.util.*;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    private int tableid;
    private int ioCostPerPage;
    private int numPages;
    private int ntups;
    private Map<Integer, IntHistogram> intHistograms;
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        HeapFile hf = ((HeapFile)Database.getCatalog().getDbFile(tableid));
        TupleDesc td = hf.getTupleDesc();
        numPages = hf.numPages();

        intHistograms = new HashMap<Integer, IntHistogram>();
        Map<Integer, Integer> mins = new HashMap<Integer, Integer>();
        Map<Integer, Integer> maxs = new HashMap<Integer, Integer>();
        for (int i = 0;i < td.numFields();i++) {
            if (td.getType(i) == Type.INT_TYPE) {
                mins.put(i, Integer.MAX_VALUE);
                maxs.put(i, Integer.MIN_VALUE);
            }
        }

        ntups = 0;
        DbFileIterator dfi = hf.iterator(new TransactionId());

        try {
            dfi.open();
            try{
                while(dfi.hasNext()) {
                    Tuple t = dfi.next();
                    for (int i = 0;i < td.numFields();i++) {
                        if (td.getType(i) == Type.INT_TYPE) {
                            int val = ((IntField)t.getField(i)).getValue();
                            mins.put(i, Math.min(mins.get(i), val));
                            maxs.put(i, Math.max(maxs.get(i), val));
                        }
                    }
                    ntups++;
                }
            }
            finally {
                dfi.close();
            }

            for (int i = 0;i < td.numFields();i++) {
                if (td.getType(i) == Type.INT_TYPE) {
                    if (mins.get(i) <= maxs.get(i))
                        intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, mins.get(i), maxs.get(i)));
                    else 
                        intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, Integer.MIN_VALUE, Integer.MAX_VALUE));
                }
            }

            dfi.rewind();
            try{
                while(dfi.hasNext()) {
                    Tuple t = dfi.next();
                    for (int i = 0;i < td.numFields();i++) {
                        if (td.getType(i) == Type.INT_TYPE) {
                            int val = ((IntField)t.getField(i)).getValue();
                            intHistograms.get(i).addValue(val);
                        }
                    }
                }
            }
            finally {
                dfi.close();
            }

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
        return numPages * ioCostPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
        return (int)(ntups * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	// some code goes here
        if (intHistograms.containsKey(field)) {
            return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }
        return 1.0;
    }

}
