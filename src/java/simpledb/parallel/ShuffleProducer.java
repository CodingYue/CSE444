package simpledb.parallel;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    private WorkingThread runningThread;

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

    class WorkingThread extends Thread {
        public void run() {

            Map<String, IoSession> workerIdToSession = new HashMap<String, IoSession>();
            Map<String, ArrayList<Tuple>> workerIdToBuffer = new HashMap<String, ArrayList<Tuple>>();
            Map<String, Long> workerIdToLastTime = new HashMap<String, Long>();
            for (SocketInfo worker : workers) {
                workerIdToSession.put(worker.getId(), ParallelUtility.createSession(
                        worker.getAddress(), getThisWorker().minaHandler, -1));
                workerIdToBuffer.put(worker.getId(), new ArrayList<Tuple>());
                workerIdToLastTime.put(worker.getId(), System.currentTimeMillis());
            }
            try {
                while (child.hasNext()) {
                    Tuple tup = child.next();
                    int partition = pf.partition(tup, child.getTupleDesc());
                    SocketInfo consumerWorker = workers[partition];
                    ArrayList<Tuple> buffer = workerIdToBuffer.get(consumerWorker.getId());
                    IoSession session = workerIdToSession.get(consumerWorker.getId());
                    long lastTime = workerIdToLastTime.get(consumerWorker.getId());
                    buffer.add(tup);
                    if (buffer.size() >= TupleBag.MAX_SIZE) {
                        session.write(new TupleBag(
                                operatorID,
                                getThisWorker().workerID,
                                buffer.toArray(new Tuple[] {}),
                                getTupleDesc()));
                        buffer.clear();
                        lastTime = System.currentTimeMillis();
                    }
                    if (buffer.size() >= TupleBag.MIN_SIZE) {
                        long thisTime = System.currentTimeMillis();
                        if (thisTime - lastTime > TupleBag.MAX_MS) {
                            session.write(new TupleBag(
                                    operatorID,
                                    getThisWorker().workerID,
                                    buffer.toArray(new Tuple[] {}),
                                    getTupleDesc()));
                            buffer.clear();
                            lastTime = thisTime;
                        }
                    }
                    workerIdToLastTime.put(consumerWorker.getId(), lastTime);
                }
                for (SocketInfo worker : workers) {
                    ArrayList<Tuple> buffer = workerIdToBuffer.get(worker.getId());
                    IoSession session = workerIdToSession.get(worker.getId());
                    if (buffer.size() > 0) {
                        session.write(new TupleBag(
                                operatorID,
                                getThisWorker().workerID,
                                buffer.toArray(new Tuple[] {}),
                                getTupleDesc()));
                    }
                    session.write(new TupleBag(operatorID,
                            getThisWorker().workerID)).addListener(new IoFutureListener<WriteFuture>(){
                        @Override
                        public void operationComplete(WriteFuture future) {
                            ParallelUtility.closeSession(future.getSession());
                        }});
                }
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
            System.out.println("Shuffle Producer finished");
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        runningThread = new WorkingThread();
        System.out.println("Producer open " + this);
        runningThread.run();
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
        try {
            runningThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
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
