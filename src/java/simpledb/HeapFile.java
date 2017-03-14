package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public synchronized Page readPage(PageId pid) throws NoSuchElementException {
        try {
            if (pid.pageNumber() == numPages()) {
                Page page = new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
                writePage(page);
                return page;
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
                byte[] data = new byte[BufferPool.PAGE_SIZE];
                randomAccessFile.read(data);
                return new HeapPage((HeapPageId) pid, data);
            }
        } catch (Exception e) {
            throw new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public synchronized void writePage(Page page) throws IOException {
        RandomAccessFile randomAccessFile;
        randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(BufferPool.PAGE_SIZE * page.getId().pageNumber());
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public synchronized int numPages() {
        return (int) file.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < numPages(); ++i) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                return new ArrayList<Page>(Arrays.asList(page));
            } else {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        if (t.getRecordId() == null) {
            throw new DbException("Heap file delete tuple, record id is null");
        }
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

