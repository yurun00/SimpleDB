package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {
    private DbIterator child;
    private TupleDesc td;
    private Aggregator agg;
    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) 
        throws DbException, TransactionAbortedException, NoSuchElementException {
        // some code goes here
        TupleDesc ctd = child.getTupleDesc();
        String aFieldName = ctd.getFieldName(afield) == null ? null : aggName(aop) + "(" + ctd.getFieldName(afield) + ")";
        if (gfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[]{ctd.getType(afield)}, 
                new String[]{aFieldName});
            agg = (ctd.getType(afield) == Type.INT_TYPE) ? 
                new IntAggregator(Aggregator.NO_GROUPING, null, afield, aop) :
                new StringAggregator(Aggregator.NO_GROUPING, null, afield, aop);
        }
        else {
            td = new TupleDesc(new Type[]{ctd.getType(gfield), ctd.getType(afield)}, 
                new String[]{ctd.getFieldName(gfield), aFieldName});
            agg = (ctd.getType(afield) == Type.INT_TYPE) ? 
                new IntAggregator(gfield, td.getType(0), afield, aop) :
                new StringAggregator(gfield, td.getType(0), afield, aop);
        }
        try {
            child.open();
            while (child.hasNext()) {
                agg.merge(child.next());
            }
            child.close();
        }
        catch(Exception e) {

        }
        this.child = agg.iterator();
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        child.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(child.hasNext()) {
            return child.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void close() {
        // some code goes here
        child.close();
    }
}
