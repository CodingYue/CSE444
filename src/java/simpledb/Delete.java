package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private DbIterator child;

    private boolean hasBeenCalled;

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

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasBeenCalled = false;
    }

    @Override
    public void close() {
        super.close();
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
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        assert children.length == 1;
        child = children[0];
    }

}
