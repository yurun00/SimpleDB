package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private TupleDesc td;
    private HashMap<Field, Integer> cnts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
        if (what != Op.COUNT)
            throw new IllegalArgumentException();

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.cnts = new HashMap<Field, Integer>();
        td = (gbfield == NO_GROUPING) ? new TupleDesc(new Type[]{Type.INT_TYPE}) :
            new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
        Field key = (gbfield == NO_GROUPING) ? DUMMY_FIELD : tup.getField(gbfield);
        if (cnts.containsKey(key))
            cnts.put(key, cnts.get(key)+1);
        else
            cnts.put(key, 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("implement me");
        return new DbIterator() {
            private Iterator<Field> child;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                child = cnts.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return child != null && child.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple t = new Tuple(td);
                Field key = child.next();
                Integer val = cnts.get(key);
                if (gbfield == NO_GROUPING)
                    t.setField(0, new IntField(val));
                else {
                    t.setField(0, key);
                    t.setField(1, new IntField(val));
                }
                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                child = cnts.keySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                child = null;
            }
        };
    }

}
