package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
    private TransactionId tid;
    private DbIterator child;
    private boolean called;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        tid = t;
        this.child = child;
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
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called)
            return null;
        called = true;

        int cnt = 0;
        BufferPool bp = Database.getBufferPool();
        while(child.hasNext()) {
            try {
                Tuple t = child.next();
                bp.deleteTuple(tid, t);
                cnt++;
            }
            catch (Exception e) {
                throw new DbException("Delete failed.");
            }
        }
        Tuple t = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        t.setField(0, new IntField(cnt));
        return t;
    }
}
