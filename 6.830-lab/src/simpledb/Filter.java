package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {
    private DbIterator child;
    private Predicate p;
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        child.open();
    }

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        while (child.hasNext()) {
            Tuple t = child.next();
            if (p.filter(t))
                return t;
        }
        return null;
    }
}
