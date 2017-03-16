package simpledb.parallel;

import java.util.*;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some
 * PartitionFunction, while the ShuffleConsumer (this class) encapsulates the
 * methods to collect the tuples received at the worker from multiple source
 * workers' ShuffleProducer.
 * 
 * */
public class ShuffleConsumer extends Consumer {

    private static final long serialVersionUID = 1L;

    /**
     * innerBufferIndex and innerBuffer are used to buffer
     * all the TupleBags this operator has received. We need
     * this because we need to support rewind.
     * */
    private transient int innerBufferIndex;
    private transient ArrayList<TupleBag> innerBuffer;


    private TupleDesc td;
    private DbIterator child;
    private final BitSet workerEOS;
    private final SocketInfo[] workers;
    private final Map<String, Integer> workerIdToIndex;

    private transient Iterator<Tuple> tuples;

    public String getName() {
        return "shuffle_c";
    }

    public ShuffleConsumer(ParallelOperatorID operatorID, SocketInfo[] workers) {
        this(null, operatorID, workers);
    }

    public ShuffleConsumer(ShuffleProducer child,
            ParallelOperatorID operatorID, SocketInfo[] workers) {
        super(operatorID);
        this.child = child;
        this.workers = workers;
        if (child != null) {
            this.td = child.getTupleDesc();
        }
        workerIdToIndex = new HashMap<String, Integer>();
        int idx = 0;
        for (SocketInfo w : workers) {
            this.workerIdToIndex.put(w.getId(), idx++);
        }
        this.workerEOS = new BitSet(workers.length);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.tuples = null;
        this.innerBuffer = new ArrayList<TupleBag>();
        this.innerBufferIndex = 0;
        if (this.child != null) {
            this.child.open();
        }
        super.open();
        if (child != null) {
            child.open();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        this.setBuffer(null);
        this.tuples = null;
        this.innerBufferIndex = -1;
        this.innerBuffer = null;
        this.workerEOS.clear();
        if (child != null) {
            child.close();
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * 
     * Retrieve a batch of tuples from the buffer of ExchangeMessages. Wait if
     * the buffer is empty.
     * 
     * @return Iterator over the new tuples received from the source workers.
     *         Return <code>null</code> if all source workers have sent an end
     *         of file message.
     */
    Iterator<Tuple> getTuples() throws InterruptedException {
        if (innerBufferIndex < innerBuffer.size()) {
            return this.innerBuffer.get(this.innerBufferIndex++).iterator();
        }
        while (this.workerEOS.nextClearBit(0) < this.workers.length) {
            TupleBag tb = (TupleBag) this.take(-1);
            if (tb.isEos()) {
                workerEOS.set(workerIdToIndex.get(tb.getWorkerID()));
            } else {
                innerBuffer.add(tb);
                innerBufferIndex++;
                return tb.iterator();
            }
        }
        return null;
    }



    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        while (tuples == null || !tuples.hasNext()) {
            try {
                tuples = getTuples();
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new DbException(e.getLocalizedMessage());
            }
            if (tuples == null) {
                return null;
            }
        }
        return tuples.next();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        assert children.length == 1;
        child = children[0];
        td = child.getTupleDesc();
    }

}
