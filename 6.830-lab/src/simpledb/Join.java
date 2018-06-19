package simpledb;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends AbstractDbIterator {
    private JoinPredicate p;
    private DbIterator child1, child2;
    private Tuple t1, t2;
    /**
     * Constructor.  Accepts to children to join and the predicate
     * to join them on
     *
     * @param p The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        t1 = null;
        t2 = null;
    }

    /**
     * @see simpledb.TupleDesc#combine(TupleDesc, TupleDesc) for possible implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.combine(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        t1 = child1.hasNext() ? child1.next() : null;
        t2 = null;
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        t1 = null;
        t2 = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        t1 = child1.hasNext() ? child1.next() : null;
        t2 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no more tuples.
     * Logically, this is the next tuple in r1 cross r2 that satisfies the join
     * predicate.  There are many possible implementations; the simplest is a
     * nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of
     * Join are simply the concatenation of joining tuples from the left and
     * right relation. Therefore, if an equality predicate is used 
     * there will be two copies of the join attribute
     * in the results.  (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (t1 == null)
            return null;
        while (true) {
            while (!child2.hasNext()) {
                if (!child1.hasNext()) {
                    close();
                    return null;
                }
                else {
                    t1 = child1.next();
                    child2.rewind();
                }
            }
            t2 = child2.next();
            if (p.filter(t1, t2)) {
                TupleDesc td = getTupleDesc();
                Tuple t = new Tuple(td);
                for (int i = 0;i < t1.getTupleDesc().numFields();i++)
                    t.setField(i, t1.getField(i));
                for (int i = 0;i < t2.getTupleDesc().numFields();i++)
                    t.setField(t1.getTupleDesc().numFields() + i, t2.getField(i));
                return t;
            }
        }
    }

    public void printRes() throws DbException, TransactionAbortedException {
        rewind();
        Tuple t = readNext();
        while (t != null) {
            System.out.println(t.toString());
            t = readNext();
        }
        close();
    }
}
