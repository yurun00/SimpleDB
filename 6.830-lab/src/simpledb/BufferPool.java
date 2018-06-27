package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    private Page[] buffer;
    private int evictIdx;
    private LockManager lm;
    private Map<TransactionId, HashSet<PageId>> hasPages;
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        buffer = new Page[numPages];
        evictIdx = -1;
        lm = new LockManager();
        hasPages = new HashMap<TransactionId, HashSet<PageId>>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // Acquire the lock first
        if (!hasPages.containsKey(tid))
            hasPages.put(tid, new HashSet<PageId>());
        hasPages.get(tid).add(pid); 
        
        lm.addLock(pid.hashCode());
        try {
            lm.acquireLock(tid, pid.hashCode(), perm);
        }
        catch (Exception e) {
            System.out.println("Error in getPage, " + e.getMessage());
        }

        // Load the page and return
        int emptyIdx = -1;
        for (int i = 0;i < buffer.length;i++) {
            if (buffer[i] == null) {
                if (emptyIdx == -1)
                    emptyIdx = i;
                break;
            }
            else if (buffer[i].getId().equals(pid)) {
                return buffer[i];
            }
        }
        if (emptyIdx < 0) {
            evictPage();
            return getPage(tid, pid, perm);
        }
        else {
            buffer[emptyIdx] = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
            return buffer[emptyIdx];
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lm.releaseLock(tid, pid.hashCode());
        /*for (int i = 0;i < buffer.length;i++) {
            if (buffer[i] != null && buffer[i].getId().equals(pid))
                
        }*/
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        for (int i = 0;i < buffer.length;i++) {
            if (buffer[i] != null && buffer[i].getId().equals(pid)) {
                return lm.holdsLock(tid, i);
            }
        }
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        /*if (commit) {
            flushPages(tid);
        }*/
        if (!commit) {
            for (int i = 0;i < buffer.length;i++) {
                if (buffer[i] != null && 
                    (tid.equals(buffer[i].isDirty()) || lm.holdsWriteLock(tid, buffer[i].getId().hashCode()))) {
                    buffer[i] = null;
                }
            }
        }
        Iterator<PageId> it = hasPages.get(tid).iterator();
        while(it.hasNext()) {
            releasePage(tid, it.next());
        }
        hasPages.remove(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pgAr = Database.getCatalog().getDbFile(tableId).addTuple(tid, t);
        for (Page pg: pgAr)
            pg.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableid = t.getRecordId().getPageId().getTableId();
        Page pg = Database.getCatalog().getDbFile(tableid).deleteTuple(tid, t);
        pg.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException, DbException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0;i < buffer.length;i++) {
            try{
                flushPage(i);
            }
            catch (Exception e) {
                System.out.println("Exception in flushAllPages(). " + e.getMessage());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException, DbException {
        // some code goes here
        // not necessary for lab1
        int i = 0;
        for (;i < buffer.length;i++) {
            if (buffer[i] != null && buffer[i].getId().equals(pid) && buffer[i].isDirty() != null) {
                HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(pid.getTableId());
                hf.writePage(buffer[i]);
                buffer[i].markDirty(false, null);
                break;
            }
        }
    }

    private synchronized  void flushPage(int pgIdx) throws IOException, DbException {
        if (buffer[pgIdx] != null && buffer[pgIdx].isDirty() != null) {
            HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(buffer[pgIdx].getId().getTableId());
            hf.writePage(buffer[pgIdx]);
            buffer[pgIdx].markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
        for (int i = 0;i < buffer.length;i++) {
            if (buffer[i] != null && hasPages.get(tid).contains(buffer[i].getId()) && tid.equals(buffer[i].isDirty())) {
                HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(buffer[i].getId().getTableId());
                hf.writePage(buffer[i]);
                buffer[i].markDirty(false, null);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * For the sake of NO STEAL policy, dirty pages and pages with write lock cannot be 
     * evicted. If no page can be evicted, throw a DbException.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int i = 0;
        for (;i < buffer.length;i++) {
            evictIdx = (evictIdx+1) % buffer.length;
            if (buffer[evictIdx] == null || 
                (buffer[evictIdx].isDirty() == null && !lm.writeLockHeld(buffer[evictIdx].getId().hashCode()))) {
                break;
            }
        }
        //System.out.println("evict index end at " + evictIdx);
        //lm.show();
        if (i == buffer.length) {
            throw new DbException("No page can be evicted.");
        }
        if (buffer[evictIdx] != null) {
            try {
                flushPage(evictIdx);
                buffer[evictIdx] = null;
                return ;
            }
            catch (Exception e) {
                throw new DbException(e.getMessage());
            } 
        }
    }

}
