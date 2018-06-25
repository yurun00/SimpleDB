package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
	private Object[] mutexes;
	private List<Set<TransactionId>> readLockHolders; 
	private List<TransactionId> writeLockHolders;
	private int[] writers;

	public LockManager(int numPages) {
		mutexes = new Object[numPages];
		readLockHolders = new ArrayList<Set<TransactionId>>(numPages);
		writeLockHolders = new ArrayList<TransactionId>(numPages);
		writers = new int[numPages];
		for (int i = 0;i < numPages;i++) {
			mutexes[i] = new Object();
			readLockHolders.add(new HashSet<TransactionId>());
			writeLockHolders.add(null);
		}
	}

	public boolean holdsReadLock(TransactionId tid, int pgIdx) {
		synchronized(mutexes[pgIdx]) {
			return readLockHolders.get(pgIdx).contains(tid);
		}
	}

	public boolean holdsWriteLock(TransactionId tid, int pgIdx) {
		synchronized(mutexes[pgIdx]) {
			return writeLockHolders.get(pgIdx) != null && writeLockHolders.get(pgIdx).equals(tid);
		}
	}

	public boolean holdsLock(TransactionId tid, int pgIdx) {
		return holdsReadLock(tid, pgIdx) || holdsWriteLock(tid, pgIdx);
	}

	boolean hasOtherWriters(TransactionId tid, int pgIdx) {
		synchronized(mutexes[pgIdx]) {
			return  writeLockHolders.get(pgIdx) != null && !writeLockHolders.get(pgIdx).equals(tid);
		}
	}

	public boolean acquireReadLock(TransactionId tid, int pgIdx) throws InterruptedException {
		// Transaction already holds the read/write lock on this page.
		if (holdsReadLock(tid, pgIdx))
			return false;
		synchronized(mutexes[pgIdx]) {
			// If some other transaction holds the write lock on this page, this transaction will be blocked.
			while (hasOtherWriters(tid, pgIdx)) {
				mutexes[pgIdx].wait();
			}
			readLockHolders.get(pgIdx).add(tid);
			return true;
		}
	}

	public boolean releaseReadLock(TransactionId tid, int pgIdx) {
		// Transaction does not hold the read lock on this page.
		if (!holdsReadLock(tid, pgIdx))
			return false;
		synchronized(mutexes[pgIdx]) {
			readLockHolders.get(pgIdx).remove(tid);
			if (readLockHolders.get(pgIdx).isEmpty())
				mutexes[pgIdx].notify();
			return true;
		}
	}

	boolean hasOtherReaders(TransactionId tid, int pgIdx) {
		synchronized(mutexes[pgIdx]) {
			return  readLockHolders.get(pgIdx).size() > 1 || 
				(readLockHolders.get(pgIdx).size() == 1 && !readLockHolders.get(pgIdx).contains(tid));
		}
	}

	public boolean acquireWriteLock(TransactionId tid, int pgIdx) throws InterruptedException {
		// Transaction already holds the write lock on this page.
		if (holdsWriteLock(tid, pgIdx)) {
			return false;
		}
		synchronized(mutexes[pgIdx]) {
			writers[pgIdx]++;
			while (hasOtherReaders(tid, pgIdx) || writeLockHolders.get(pgIdx) != null) {
				mutexes[pgIdx].wait();
			}
			// Upgrade the read lock to the write lock
			readLockHolders.get(pgIdx).remove(tid);
			writeLockHolders.set(pgIdx, tid);
			return true;
		}
	}

	public boolean releaseWriteLock(TransactionId tid, int pgIdx) {
		// Transaction does not hold the write lock on this page.
		if (!holdsWriteLock(tid, pgIdx))
			return false;
		synchronized(mutexes[pgIdx]) {
			writers[pgIdx]--;
			writeLockHolders.set(pgIdx, null);
			mutexes[pgIdx].notifyAll();
			return true;
		}
	}

	public boolean acquireLock(TransactionId tid, int pgIdx, Permissions perm) throws InterruptedException {
		try {
			if (perm.equals(Permissions.READ_ONLY)) 
				return acquireReadLock(tid, pgIdx);
			else 
				return acquireWriteLock(tid, pgIdx);
		}
		catch (InterruptedException e) {
			for (int i = 0;i < mutexes.length;i++) {
				releaseLock(tid, i);
			}
			if (perm.equals(Permissions.READ_WRITE)) {
				synchronized (mutexes[pgIdx]) {
					writers[pgIdx]--;
				}
			}
			throw new InterruptedException();
		}
	}

	public boolean releaseLock(TransactionId tid, int pgIdx) {
		return releaseReadLock(tid, pgIdx) || releaseWriteLock(tid, pgIdx);
	}
}