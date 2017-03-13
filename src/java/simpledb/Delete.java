package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private final DbIterator child;

    private boolean hasBeenCalled;

    private DbIterator[] children;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.transactionId = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        hasBeenCalled = false;
    }

    public void close() {
        child.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasBeenCalled) {
            return null;
        }
        hasBeenCalled = true;
        int deleteCount = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            deleteCount++;
            Database.getBufferPool().deleteTuple(transactionId, tuple);
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(deleteCount));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.children = children;
    }

}
