package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
public class HeapFileIterator implements DbFileIterator {
    private final HeapFile heapFile;
    private final TransactionId transactionId;
    private int currentPageNo;
    private Iterator<Tuple> pageIterator;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        heapFile = hf;
        transactionId = tid;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
            throws DbException, TransactionAbortedException {
        currentPageNo = 0;
        pageIterator = getTupleIteratorByPageNo(currentPageNo);
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext()
            throws DbException, TransactionAbortedException {
        if (pageIterator == null) {
            return false;
        }
        while (!pageIterator.hasNext()) {
            ++currentPageNo;
            if (currentPageNo >= heapFile.numPages()) {
                return false;
            }
            pageIterator = getTupleIteratorByPageNo(currentPageNo);
        }
        return true;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return pageIterator.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        currentPageNo = 0;
        pageIterator = getTupleIteratorByPageNo(currentPageNo);
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        pageIterator = null;
    }

    private Iterator<Tuple> getTupleIteratorByPageNo(int pageNo)
            throws NoSuchElementException, TransactionAbortedException, DbException {
        HeapPage page;
        page = (HeapPage) Database.getBufferPool().getPage(
                transactionId, new HeapPageId(heapFile.getId(), pageNo), Permissions.READ_ONLY);
        return page.iterator();
    }
}