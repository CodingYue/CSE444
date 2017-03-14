package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private final DbIterator child;
    private final int tableId;

    private boolean hasBeenCalled;

    private DbIterator[] children;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableid;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        this.hasBeenCalled = false;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasBeenCalled) {
            return null;
        }
        int insertCount = 0;
        hasBeenCalled = true;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            insertCount++;
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
            } catch (IOException e) {
                throw new DbException("Insert failed");
            }
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(insertCount));
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
