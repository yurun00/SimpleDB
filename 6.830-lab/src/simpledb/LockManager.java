package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
	private Map<Integer, Object> mutexes;
	private Map<Integer, HashSet<TransactionId>> readLockHolders; 
	private Map<Integer, TransactionId> writeLockHolders;
	private Map<Integer, Integer> writers;

	//private evicted

	public LockManager() {
		mutexes = new HashMap<Integer, Object>();
		readLockHolders = new HashMap<Integer, HashSet<TransactionId>>();
		writeLockHolders = new HashMap<Integer, TransactionId>();
		writers = new HashMap<Integer, Integer>();
	}

	public void show() {
		System.out.println("----------Lock manager content----------");
		System.out.println("Pages hash:");
		Iterator<Integer> it = mutexes.keySet().iterator();
		while (it.hasNext())
			System.out.println(it.next());

		System.out.println("----------");
		System.out.println("Read lock holders: ");
		it = readLockHolders.keySet().iterator();
		while (it.hasNext()) {
			int ph = it.next();
			String s = "";
			Iterator<TransactionId> it1 = readLockHolders.get(ph).iterator();
			while (it1.hasNext())
				s += ", " + it1.next().getId();
			System.out.println("page hash: " + ph + s);
		}

		System.out.println("----------");
		System.out.println("Write lock holders: ");
		it = writeLockHolders.keySet().iterator();
		while (it.hasNext()) {
			int ph = it.next();
			TransactionId ti = writeLockHolders.get(ph);
			System.out.println("page hash: " + ph + ", " + (ti != null ? ti.getId() : null));
		}

		System.out.println("----------");
		System.out.println("Writers number: ");
		it = writers.keySet().iterator();
		while (it.hasNext()) {
			int ph = it.next();
			System.out.println("page hash: " + ph + ", " + writers.get(ph));
		}
	}

	public synchronized void addLock(int pghc) {
		if (!mutexes.containsKey(pghc)) {
			mutexes.put(pghc, new Object());
			readLockHolders.put(pghc, new HashSet<TransactionId>());
			writeLockHolders.put(pghc, null);
			writers.put(pghc, 0);
		}
	}

	public boolean writeLockHeld(int pghc) {
		synchronized(mutexes.get(pghc)) {
			return writeLockHolders.get(pghc) != null;
		}
	}

	private boolean holdsReadLock(TransactionId tid, int pghc) {
		synchronized(mutexes.get(pghc)) {
			return readLockHolders.get(pghc).contains(tid);
		}
	}

	public boolean holdsWriteLock(TransactionId tid, int pghc) {
		synchronized(mutexes.get(pghc)) {
			return writeLockHolders.get(pghc) != null && writeLockHolders.get(pghc).equals(tid);
		}
	}

	public boolean holdsLock(TransactionId tid, int pghc) {
		return holdsReadLock(tid, pghc) || holdsWriteLock(tid, pghc);
	}

	private boolean hasOtherWriters(TransactionId tid, int pghc) {
		synchronized(mutexes.get(pghc)) {
			return  writeLockHolders.get(pghc) != null && !writeLockHolders.get(pghc).equals(tid);
		}
	}

	private boolean acquireReadLock(TransactionId tid, int pghc) throws InterruptedException {
		// Transaction already holds the read/write lock on this page.
		if (holdsReadLock(tid, pghc))
			return false;
		synchronized(mutexes.get(pghc)) {
			// If some other transaction holds the write lock on this page, this transaction will be blocked.
			while (hasOtherWriters(tid, pghc)) {
				mutexes.get(pghc).wait();
			}
			readLockHolders.get(pghc).add(tid);
			return true;
		}
	}

	private boolean releaseReadLock(TransactionId tid, int pghc) {
		// Transaction does not hold the read lock on this page.
		if (!holdsReadLock(tid, pghc))
			return false;
		synchronized(mutexes.get(pghc)) {
			readLockHolders.get(pghc).remove(tid);
			if (readLockHolders.get(pghc).isEmpty())
				mutexes.get(pghc).notify();
			return true;
		}
	}

	boolean hasOtherReaders(TransactionId tid, int pghc) {
		synchronized(mutexes.get(pghc)) {
			return  readLockHolders.get(pghc).size() > 1 || 
				(readLockHolders.get(pghc).size() == 1 && !readLockHolders.get(pghc).contains(tid));
		}
	}

	public boolean acquireWriteLock(TransactionId tid, int pghc) throws InterruptedException {
		// Transaction already holds the write lock on this page.
		if (holdsWriteLock(tid, pghc)) {
			return false;
		}
		synchronized(mutexes.get(pghc)) {
			writers.put(pghc, writers.get(pghc)+1);
			while (hasOtherReaders(tid, pghc) || writeLockHolders.get(pghc) != null) {
				mutexes.get(pghc).wait();
			}
			// Upgrade the read lock to the write lock
			readLockHolders.get(pghc).remove(tid);
			writeLockHolders.put(pghc, tid);
			return true;
		}
	}

	public boolean releaseWriteLock(TransactionId tid, int pghc) {
		// Transaction does not hold the write lock on this page.
		if (!holdsWriteLock(tid, pghc))
			return false;
		synchronized(mutexes.get(pghc)) {
			writers.put(pghc, writers.get(pghc)-1);
			writeLockHolders.put(pghc, null);
			mutexes.get(pghc).notifyAll();
			return true;
		}
	}

	public boolean acquireLock(TransactionId tid, int pghc, Permissions perm) throws InterruptedException {
		try {
			if (perm.equals(Permissions.READ_ONLY)) 
				return acquireReadLock(tid, pghc);
			else 
				return acquireWriteLock(tid, pghc);
		}
		catch (InterruptedException e) {
			Iterator<Integer> it = mutexes.keySet().iterator();
			while(it.hasNext()) {
				releaseLock(tid, it.next());
			}
			if (perm.equals(Permissions.READ_WRITE)) {
				synchronized (mutexes.get(pghc)) {
					writers.put(pghc, writers.get(pghc)-1);
				}
			}
			throw new InterruptedException();
		}
	}

	public boolean releaseLock(TransactionId tid, int pghc) {
		boolean b1 = releaseReadLock(tid, pghc);
		boolean b2 = releaseWriteLock(tid, pghc);
		return b1 || b2;
	}
}