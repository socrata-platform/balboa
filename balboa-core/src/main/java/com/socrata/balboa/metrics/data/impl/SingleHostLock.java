package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.Lock;

import java.util.HashSet;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

public class SingleHostLock implements Lock
{
    private final Map<String, LockStructure> locks = new HashMap<String, LockStructure>();
    private final int maxWaitLength;
    private final long maxWaitTime;

    public SingleHostLock(int maxWaitLength, long maxWaitTime) {
        this.maxWaitLength = maxWaitLength;
        this.maxWaitTime = maxWaitTime;
    }

    private static class LockStructure {                                                                 
        Thread heldBy = null;
        int holdCount = 0;
        Date heldSince = null;
        final HashSet<Thread> waiters = new HashSet<Thread>();

        void acquiredBy(Thread newOwner) {
            // PRECONDITION: either I own the lock's monitor, or I hold the only reference to it.
            heldBy = newOwner;
            holdCount += 1;
            heldSince = new Date();
            waiters.remove(newOwner);
        }
    }

    @Override
    public int delay() {
        return 0;
    }

    private boolean canOwnLock(LockStructure lock, Thread th) {
        return lock.heldBy == null /* || th == lock.heldBy */;
    }

    @Override
    public boolean acquire(String name) {
        long deadline = System.currentTimeMillis() + maxWaitTime;

        Thread self = Thread.currentThread();
        LockStructure lock;
        synchronized(this) {
            lock = locks.get(name);
            if(lock == null) {
                lock = new LockStructure();
                locks.put(name, lock);
                lock.acquiredBy(self);
                return true;
            }
            synchronized(lock) {
                if(!lock.waiters.contains(self)) {
                    if(lock.heldBy != self && lock.waiters.size() >= maxWaitLength) return false;
                    lock.waiters.add(self);
                }
            }
        }

        synchronized(lock) {
            while(!canOwnLock(lock, self) && (deadline - System.currentTimeMillis() > 0L)) {
                try {
                    lock.wait(Math.max(deadline - System.currentTimeMillis(), 1L));
                } catch(InterruptedException e) {
                    synchronized(lock) {
                        if(!canOwnLock(lock, self)) {
                            lock.waiters.remove(self);
                            return false;
                        } else {
                            lock.acquiredBy(self);
                            return true;
                        }
                    }
                }
            }
            if(!canOwnLock(lock, self)) {
                lock.waiters.remove(self);
                return false;
            }
            lock.acquiredBy(self);
        }

        return true;
    }

    @Override
    public void release(String name) {
        synchronized(this) {
            LockStructure lock = locks.get(name);
            if(lock == null) return;
            synchronized(lock) {
                lock.holdCount -= 1;
                if(lock.holdCount == 0) {
                    if(lock.waiters.isEmpty()) {
                        locks.remove(name);
                    } else {
                        lock.heldBy = null;
                        lock.notify();
                    }
                }
            }
        }
    }
}
