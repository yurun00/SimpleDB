package simpledb;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private boolean called;
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        if (child.getTupleDesc().equals(td))
        tid = t;
        this.child = child;
        this.tableid = tableid;
        called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
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
        called = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        // some code goes here
        if (called)
            return null;
        called = true;

        BufferPool bp = Database.getBufferPool();
        int cnt = 0;
        while (child.hasNext()) {
            try {
                Tuple t = child.next();
                bp.insertTuple(tid, tableid, t);
                cnt++;
            }
            catch (TransactionAbortedException e) {
                throw e;
            }
            catch (Exception e) {
                throw new DbException("Insert failed.");
            }
        }
        Tuple t = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        t.setField(0, new IntField(cnt));
        return t;
    }
}
