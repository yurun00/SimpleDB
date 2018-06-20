package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private TupleDesc td;
    private HashMap<Field, Integer> vals;
    private HashMap<Field, Integer> cnts;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.vals = new HashMap<Field, Integer>();
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
        if (vals.containsKey(key)) {
            switch (what) {
                case MIN:
                    vals.put(key, Math.min(vals.get(key), tup.getField(afield).hashCode()));
                    break;
                case MAX:
                    vals.put(key, Math.max(vals.get(key), tup.getField(afield).hashCode()));
                    break;
                case SUM:
                    vals.put(key, vals.get(key) + tup.getField(afield).hashCode());
                    break;
                case AVG:
                    vals.put(key, vals.get(key) + tup.getField(afield).hashCode());
                    break;
                case COUNT:
                    vals.put(key, cnts.get(key));
                    break;
                default:
                    vals.put(key, vals.get(key) + tup.getField(afield).hashCode());
            }
        }
        else {
            switch (what) {
                case MIN:
                    vals.put(key, tup.getField(afield).hashCode());
                    break;
                case MAX:
                    vals.put(key, tup.getField(afield).hashCode());
                    break;
                case SUM:
                    vals.put(key, tup.getField(afield).hashCode());
                    break;
                case AVG:
                    vals.put(key, tup.getField(afield).hashCode());
                    break;
                case COUNT:
                    vals.put(key, cnts.get(key));
                    break;
                default:
                    vals.put(key, tup.getField(afield).hashCode());
            }
        }
    }

    public void printContent() {
        Iterator<Field> it = vals.keySet().iterator();
        while (it.hasNext()) {
            Field key = it.next();
            System.out.println(key.toString() + ": " + cnts.get(key));
        }
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
                child = vals.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return child != null && child.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple t = new Tuple(td);
                Field key = child.next();
                Integer val = 0;
                switch (what) {
                    case MIN: case MAX: case SUM:
                        val = vals.get(key);
                        break;
                    case AVG:
                        val = vals.get(key) / cnts.get(key);
                        break;
                    case COUNT:
                        val = cnts.get(key);
                        break;
                    default:
                        val = vals.get(key);   
                }

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
                child = vals.keySet().iterator();
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
