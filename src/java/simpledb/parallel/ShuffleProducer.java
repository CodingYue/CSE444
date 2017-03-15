package simpledb.parallel;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;

    private final ParallelOperatorID operatorID;
    private final SocketInfo[] workers;

    private PartitionFunction<?, ?> pf;
    private DbIterator child;

    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(DbIterator child, ParallelOperatorID operatorID,
            SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        this.child = child;
        this.operatorID = operatorID;
        this.workers = workers;
        this.pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        this.pf = pf;
    }

    public SocketInfo[] getWorkers() {
        return workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        return pf;
    }

    // some code goes here
    class WorkingThread extends Thread {
        public void run() {

            // some code goes here
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        return child.hasNext() ? child.next() : null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        assert children.length == 1;
        this.child = children[0];
    }
}
