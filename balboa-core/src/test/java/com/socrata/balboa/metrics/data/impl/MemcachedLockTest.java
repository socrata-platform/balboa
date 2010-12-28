package com.socrata.balboa.metrics.data.impl;

import net.spy.memcached.*;
import net.spy.memcached.transcoders.Transcoder;
import org.junit.Assert;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MemcachedLockTest
{
    static class TheNotSoDistantFuture<V> implements Future
    {
        V value;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException
        {
            return value;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return value;
        }
    }

    // Good lord, could their interface be any longer?
    static class FakeMCClient implements MemcachedClientIF
    {
        Map<String, Object> cache = new HashMap<String, Object>();

        @Override
        public Set<String> listSaslMechanisms()
        {
            return null;
        }

        @Override
        public Collection<SocketAddress> getAvailableServers()
        {
            // Don't care about this.
            return null;
        }

        @Override
        public Collection<SocketAddress> getUnavailableServers()
        {
            // Don't care about this.
            return null;
        }

        @Override
        public Transcoder<Object> getTranscoder()
        {
            // Don't care about this.
            return null;
        }

        @Override
        public NodeLocator getNodeLocator()
        {
            // Don't care about this.
            return null;
        }

        @Override
        public Future<Boolean> append(long l, String s, Object o)
        {
            return null;
        }

        @Override
        public <T> Future<Boolean> append(long l, String s, T t, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<Boolean> prepend(long l, String s, Object o)
        {
            return null;
        }

        @Override
        public <T> Future<Boolean> prepend(long l, String s, T t, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public <T> Future<CASResponse> asyncCAS(String s, long l, T t, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<CASResponse> asyncCAS(String s, long l, Object o)
        {
            return null;
        }

        @Override
        public <T> CASResponse cas(String s, long l, T t, Transcoder<T> tTranscoder) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public CASResponse cas(String s, long l, Object o) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public <T> Future<Boolean> add(String s, int i, T t, Transcoder<T> tTranscoder)
        {
            return add(s, i, t);
        }

        @Override
        public Future<Boolean> add(String s, int i, Object o)
        {
            TheNotSoDistantFuture<Boolean> nextSundayAd = new TheNotSoDistantFuture<Boolean>();

            if (cache.containsKey(s))
            {
                nextSundayAd.value = false;
            }
            else
            {
                cache.put(s, o);
                nextSundayAd.value = true;
            }

            return nextSundayAd;
        }

        @Override
        public <T> Future<Boolean> set(String s, int i, T t, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<Boolean> set(String s, int i, Object o)
        {
            return null;
        }

        @Override
        public <T> Future<Boolean> replace(String s, int i, T t, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<Boolean> replace(String s, int i, Object o)
        {
            return null;
        }

        @Override
        public <T> Future<T> asyncGet(String s, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<Object> asyncGet(String s)
        {
            return null;
        }

        @Override
        public <T> Future<CASValue<T>> asyncGets(String s, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<CASValue<Object>> asyncGets(String s)
        {
            return null;
        }

        @Override
        public <T> CASValue<T> gets(String s, Transcoder<T> tTranscoder) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public CASValue<Object> gets(String s) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public <T> T get(String s, Transcoder<T> tTranscoder) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public Object get(String s) throws OperationTimeoutException
        {
            return cache.get(s);
        }

        @Override
        public <T> Future<Map<String, T>> asyncGetBulk(Collection<String> strings, Transcoder<T> tTranscoder)
        {
            return null;
        }

        @Override
        public Future<Map<String, Object>> asyncGetBulk(Collection<String> strings)
        {
            return null;
        }

        @Override
        public <T> Future<Map<String, T>> asyncGetBulk(Transcoder<T> tTranscoder, String... strings)
        {
            return null;
        }

        @Override
        public Future<Map<String, Object>> asyncGetBulk(String... strings)
        {
            return null;
        }

        @Override
        public <T> Map<String, T> getBulk(Collection<String> strings, Transcoder<T> tTranscoder) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public Map<String, Object> getBulk(Collection<String> strings) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public <T> Map<String, T> getBulk(Transcoder<T> tTranscoder, String... strings) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public Map<String, Object> getBulk(String... strings) throws OperationTimeoutException
        {
            return null;
        }

        @Override
        public Map<SocketAddress, String> getVersions()
        {
            return null;
        }

        @Override
        public Map<SocketAddress, Map<String, String>> getStats()
        {
            return null;
        }

        @Override
        public Map<SocketAddress, Map<String, String>> getStats(String s)
        {
            return null;
        }

        @Override
        public long incr(String s, int i) throws OperationTimeoutException
        {
            return 0;
        }

        @Override
        public long decr(String s, int i) throws OperationTimeoutException
        {
            return 0;
        }

        @Override
        public long incr(String s, int i, long l, int i1) throws OperationTimeoutException
        {
            return 0;
        }

        @Override
        public long decr(String s, int i, long l, int i1) throws OperationTimeoutException
        {
            return 0;
        }

        @Override
        public Future<Long> asyncIncr(String s, int i)
        {
            return null;
        }

        @Override
        public Future<Long> asyncDecr(String s, int i)
        {
            return null;
        }

        @Override
        public long incr(String s, int i, long l) throws OperationTimeoutException
        {
            return 0;
        }

        @Override
        public long decr(String s, int i, long l) throws OperationTimeoutException
        {
            return 0;
        }

        @Override
        public Future<Boolean> delete(String s)
        {
            TheNotSoDistantFuture<Boolean> nextSundayAD = new TheNotSoDistantFuture<Boolean>();

            if (cache.containsKey(s))
            {
                cache.remove(s);
                nextSundayAD.value = true;
            }
            else
            {
                nextSundayAD.value = false;
            }

            return nextSundayAD;
        }

        @Override
        public Future<Boolean> flush(int i)
        {
            return null;
        }

        @Override
        public Future<Boolean> flush()
        {
            return null;
        }

        @Override
        public void shutdown()
        {
        }

        @Override
        public boolean shutdown(long l, TimeUnit timeUnit)
        {
            return false;
        }

        @Override
        public boolean waitForQueues(long l, TimeUnit timeUnit)
        {
            return false;
        }

        @Override
        public boolean addObserver(ConnectionObserver connectionObserver)
        {
            return false;
        }

        @Override
        public boolean removeObserver(ConnectionObserver connectionObserver)
        {
            return false;
        }
    }

    @Test
    public void testLockingCanAcquireAndRelease() throws Exception
    {
        FakeMCClient c = new FakeMCClient();
        MemcachedLock lock = new MemcachedLock(c);
        boolean acquired = lock.acquire("foo");

        Assert.assertTrue(acquired);

        Assert.assertTrue(c.cache.containsKey("balboa:lock:foo"));
        Assert.assertTrue(c.cache.get("balboa:lock:foo").equals(lock.id.toString()));

        lock.release("foo");

        Assert.assertFalse(c.cache.containsKey("balboa:lock:foo"));
    }

    @Test
    public void testReleaseLockThatHasntBeenAcquiredDoesNothing() throws Exception
    {
        FakeMCClient c = new FakeMCClient();
        MemcachedLock lock = new MemcachedLock(c);
        lock.release("whatever");
    }

    @Test
    public void testLockWontAcquireIfSomeoneAlreadyHasIt() throws Exception
    {
        FakeMCClient c = new FakeMCClient();
        MemcachedLock lock = new MemcachedLock(c);
        boolean acquired =lock.acquire("foo");

        Assert.assertTrue(acquired);

        MemcachedLock lock2 = new MemcachedLock(c);
        acquired = lock.acquire("foo");

        Assert.assertFalse(acquired);

        Assert.assertTrue(c.cache.containsKey("balboa:lock:foo"));
        Assert.assertTrue(c.cache.get("balboa:lock:foo").equals(lock.id.toString()));
    }

    @Test(expected=java.io.IOException.class)
    public void testLockingWontReleaseLockItDoesntOwn() throws Exception
    {
        FakeMCClient c = new FakeMCClient();
        MemcachedLock lock = new MemcachedLock(c);
        boolean acquired = lock.acquire("foo");

        Assert.assertTrue(acquired);

        Assert.assertTrue(c.cache.containsKey("balboa:lock:foo"));
        Assert.assertTrue(c.cache.get("balboa:lock:foo").equals(lock.id.toString()));

        c.cache.put("balboa:lock:foo", "snuffleupadata");

        lock.release("foo");
    }
}
