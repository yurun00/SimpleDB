package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
        return td;
    	//throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        if (getId() != pid.getTableId()) 
            throw new IllegalArgumentException();

        int pgNo = pid.pageno();
        if (pgNo < 0 || pgNo >= numPages()) 
            throw new IllegalArgumentException();

        byte[] data = HeapPage.createEmptyPageData();
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            try {
                raf.seek((long)pgNo * BufferPool.PAGE_SIZE);
                raf.read(data);
                return new HeapPage((HeapPageId)pid, data);
            }
            finally {
                raf.close();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
            
            
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().pageno();
        byte[] data = page.getPageData();
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            try {
                raf.seek((long)pgNo * BufferPool.PAGE_SIZE);
                raf.write(data);
            }
            finally {
                raf.close();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
        /**
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pgAr = new ArrayList<Page>();
        int tableid = getId(), pgno = 0;
        for (;pgno < numPages();pgno++) {
            PageId pid = new HeapPageId(tableid, pgno);
            HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (pg.getNumEmptySlots() > 0) {
                HeapPage wpg = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                wpg.addTuple(t);
                pgAr.add(wpg);
                return pgAr;
            }
            /*else {
                Database.getBufferPool().releasePage(tid, pg.getId());
            }*/
        }
        byte[] data = HeapPage.createEmptyPageData();
        HeapPage pg = new HeapPage(new HeapPageId(tableid, pgno), data);
        pg.addTuple(t);
        pgAr.add(pg);
        writePage(pg);
        return pgAr;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        pg.deleteTuple(t);
        return pg;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(final TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private BufferPool bp = Database.getBufferPool();
            private int tableId = getId();
            private int pgNo = -1;
            private TransactionId _tid = tid;
            private Iterator<Tuple> ti = null;

            @Override
            public void open() {
                pgNo = 0;
                ti = null;
            }

            @Override
            public void close() {
                pgNo = -1;
                ti = null;
            }

            @Override
            public boolean hasNext() throws TransactionAbortedException, DbException{
                if (pgNo < 0)
                    return false;
                if (ti == null) {
                    ti = ((HeapPage)bp.getPage(_tid, new HeapPageId(tableId, pgNo), Permissions.READ_ONLY)).iterator();
                    return hasNext();
                }
                if (ti.hasNext())
                    return true;
                else if (pgNo >= numPages()-1)
                    return false;
                else {
                    ti = ((HeapPage)bp.getPage(_tid, new HeapPageId(tableId, ++pgNo), Permissions.READ_ONLY)).iterator();
                    return hasNext();
                }
            }

            @Override
            public Tuple next() throws TransactionAbortedException, DbException, NoSuchElementException{
                if (!hasNext())
                    throw new NoSuchElementException();
                return ti.next();
            }

            @Override
            public void rewind() {
                pgNo = 0;
                ti = null;
            }
        };
    }
}

