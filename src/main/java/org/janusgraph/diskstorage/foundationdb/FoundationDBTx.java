package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.apple.foundationdb.async.AsyncIterator;
import org.janusgraph.diskstorage.tracing.CallContext;
import org.janusgraph.diskstorage.tracing.CallCtxThreadLocalHolder;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs.*;

import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsUtil;
import static java.util.stream.Collectors.toList;
import org.janusgraph.diskstorage.foundationdb.utils.LogWithCallContext;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private volatile Transaction tx;

    private final Database db;

    private List<Insert> inserts = Collections.synchronizedList(new ArrayList<>());
    private List<byte[]> deletions = Collections.synchronizedList(new ArrayList<>());

    private int maxRuns = 1;

    public enum IsolationLevel { SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE }

    private final IsolationLevel isolationLevel;

    private AtomicInteger txCtr = new AtomicInteger(0);

    private static AtomicInteger transLocalIdCounter = new AtomicInteger(0);
    private int transactionId = 0;

    private FoundationDBStoreManager storeManager;
    private long startTransactionTime;
    private final boolean readOnlyMode;

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config, IsolationLevel isolationLevel,
                          FoundationDBStoreManager storeManager, boolean readOnlyMode) {
        super(config);
        tx = t;
        this.db = db;
        this.isolationLevel = isolationLevel;
        this.readOnlyMode = readOnlyMode;

        switch (isolationLevel) {
            case SERIALIZABLE:
                // no retries
                break;
            case READ_COMMITTED_NO_WRITE:
            case READ_COMMITTED_WITH_WRITE:
                maxRuns = 3;
        }

        //add the transaction id, to see the query source
        this.transactionId = transLocalIdCounter.incrementAndGet();

        this.storeManager = storeManager;
        this.startTransactionTime = System.nanoTime();

        FoundationDBMetricsUtil.TransactionScope.incTransactionsOpened(storeManager.metricGroup, isolationLevel);
    }


    /***
     * Handle a potential FDBException. We simply log the exception (at the debug mode), and return false,
     * and let's caller to: (1) if the mode is set to: serializable, then the caller will throw PermanentBackendException;
     *     (2) if the mode is set to: either read_committed_no_write or read_committed_with_write, then the caller
     *         will invoke re-start of the whole transaction.
     * @return always false for now.
     * */
    private boolean handleFDBException(Exception e) {
        if (e instanceof ExecutionException) {
            Throwable t = e.getCause();
            if (t != null && t instanceof FDBException) {
                FDBException fe = (FDBException) t;
                if (log.isDebugEnabled()) {
                    String message= String.format("Catch FDBException code=%s, isRetryable=%s, isMaybeCommitted=%s, "
                                    + "isRetryableNotCommitted=%s, isSuccess=%s",
                            fe.getCode(), fe.isRetryable(),
                            fe.isMaybeCommitted(), fe.isRetryableNotCommitted(), fe.isSuccess());
                    LogWithCallContext.logDebug(log, message);
                }
            }
        }
        return false;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public synchronized void restart() {
        txCtr.incrementAndGet();

        if (tx != null) {
            try {
                tx.cancel();
            } catch (IllegalStateException e) {
                //
            } finally {
                try {
                    tx.close();
                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        LogWithCallContext.logError(log, "Exception when closing transaction: " + e.getMessage());
                    }
                }
            }

            FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                    storeManager.metricGroup, determineReadWrite(), isolationLevel);
        }
        else {
            if (log.isWarnEnabled()) {
                log.warn("in execution mode: {} and when restart transaction, encounter FDB transaction object to be null",
                        isolationLevel.name());
            }
        }

        tx = db.createTransaction();
        // Reapply mutations but do not clear them out just in case this transaction also
        // times out and they need to be reapplied.
        //
        // @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes) is not handled.
        inserts.forEach(insert -> tx.set(insert.getKey(), insert.getValue()));
        deletions.forEach(delete -> tx.clear(delete));

        //reset the timer
        this.startTransactionTime = System.nanoTime();

        FoundationDBMetricsUtil.TransactionScope.incTransactionsOpened(storeManager.metricGroup, isolationLevel);
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();

        String opPerformed= determineReadWrite();
        if (tx == null) {
            if (log.isWarnEnabled()) {
                log.warn("in execution mode: {} and when rollback, encounter FDB transaction object to be null", isolationLevel.name());
            }
            FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                    storeManager.metricGroup, opPerformed, isolationLevel, startTransactionTime, false);
            FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                    storeManager.metricGroup, opPerformed, isolationLevel);

            return;
        }
        if (log.isTraceEnabled()) {
            log.trace ("{} rolled back", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));
        }

        try {
            tx.cancel();
            tx.close();
            tx = null;
        } catch (Exception e) {
            FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                    storeManager.metricGroup, opPerformed, isolationLevel, startTransactionTime, false);
            FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                    storeManager.metricGroup, opPerformed, isolationLevel);
            FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                    storeManager.metricGroup, opPerformed, isolationLevel);

            throw new PermanentBackendException(e);
        } finally {
            if (tx != null) {
                try {
                    tx.close();
                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        LogWithCallContext.logError(log, "Exception when closing transaction: " + e.getMessage());
                    }
                }
                tx = null;
            }
        }

        if (LogWithCallContext.isLogAuditEnabled() && (inserts.size() > 0 || deletions.size() > 0)) {
            String msg = String.format("Transaction rolled back, num_inserts=%d, num_deletes=%d",
                    inserts.size(), deletions.size());
            LogWithCallContext.logAudit(msg);
        }

        //rollback always indicating transaction failure.
        FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                storeManager.metricGroup, opPerformed, isolationLevel, startTransactionTime, false);
        FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                storeManager.metricGroup, opPerformed, isolationLevel);
        FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                storeManager.metricGroup, opPerformed, isolationLevel);
    }

    @Override
    public synchronized void commit() throws BackendException {
        boolean failing = true;
        int counter=0;

        String opPerformed = determineReadWrite();
        for (int i = 0; i < maxRuns; i++) {
            super.commit();

            if (tx == null) {
                if (log.isWarnEnabled()) {
                    log.warn("in execution mode: {} and when commit, encounter FDB transaction object to be null", isolationLevel.name());
                }
                FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                        storeManager.metricGroup, opPerformed, isolationLevel,
                        startTransactionTime, true);
                FoundationDBMetricsUtil.TransactionScope.incTransactions(
                        storeManager.metricGroup, opPerformed, isolationLevel);

                return;
            }
            if (log.isTraceEnabled()) {
                log.trace("{} committed", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));
            }

            try {
                if (!inserts.isEmpty() || !deletions.isEmpty()) {
                    if (readOnlyMode) {
                        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
                        if (context != null) {
                            throw new IllegalStateException("FDB Plugin: writes are not allowed in read-only mode.");
                        }
                    }

                    tx.commit().get();

                    if (LogWithCallContext.isLogAuditEnabled()) {
                        String msg = String.format("Transaction committed, num_inserts=%d, num_deletes=%d",
                                inserts.size(), deletions.size());
                        LogWithCallContext.logAudit(msg);
                    }
                } else {
                    // nothing to commit so skip it
                    tx.cancel();
                }
                tx.close();
                tx = null;
                failing = false;
                break;
            } catch (IllegalStateException | ExecutionException e) {
                if (tx != null) {
                    try {
                        tx.close();
                    } catch (Exception ex) {
                        if (log.isErrorEnabled()) {
                            LogWithCallContext.logError(log, "Exception when closing transaction: " + ex.getMessage());
                        }
                    }
                    tx = null;
                }

                if (log.isErrorEnabled()) {
                    String message= "commit encounter exceptions: " + e.getMessage() + " i: " + i + " maxRuns: " + maxRuns
                            + " will be re-started for inserts: " + inserts.size() + " deletes: " + deletions.size();
                    LogWithCallContext.logError(log, message);
                }

                //report metrics first before handle the exception (that can potentially throw another exception).
                handleFDBException(e);

                //need to put the following first, as otherwise restart will reset the timer.
                FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                        storeManager.metricGroup, opPerformed, isolationLevel,
                        startTransactionTime, false);
                FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                        storeManager.metricGroup, opPerformed, isolationLevel);
                FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                        storeManager.metricGroup, opPerformed, isolationLevel);

                if (isolationLevel.equals(IsolationLevel.SERIALIZABLE) ||
                        isolationLevel.equals(IsolationLevel.READ_COMMITTED_NO_WRITE)) {
                    if (log.isErrorEnabled()) {
                        String message = String.format("commit fails: inserts=%d, deletes=%d", inserts.size(), deletions.size());
                        LogWithCallContext.logError(log, message);
                    }
                    // throw the exception that carries the root exception cause
                    throw new PermanentBackendException("transaction fails to commit", e);
                }

                if (i+1 != maxRuns) {
                    restart();
                    counter++; //to increase how many times that the commit has been re-started.
                }
            } catch (Exception e) {
                if (tx != null) {
                    try {
                        tx.close();
                    } catch (Exception ex) {
                        if (log.isErrorEnabled()) {
                            LogWithCallContext.logError(log, "Exception when closing transaction: " + ex.getMessage());
                        }
                    }
                    tx = null;
                }

                FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                        storeManager.metricGroup, opPerformed, isolationLevel,
                        startTransactionTime, false);
                FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                        storeManager.metricGroup, opPerformed, isolationLevel);
                FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                        storeManager.metricGroup, opPerformed, isolationLevel);


                if (log.isErrorEnabled()){
                    LogWithCallContext.logError(log, "commit encountered exception: " + e.getMessage());
                }
                throw new PermanentBackendException(e);
            }

        }

        if (failing) {
            //Note: we already record the counter and latency in the failure path.
            if (log.isErrorEnabled()) {
                String message = "commit has the final result with failing (should be true): " + failing + " maxRuns: " + maxRuns +
                        " inserts: " + inserts.size() + " deletes: " + deletions.size()
                        + " total re-starts: " + counter;
                LogWithCallContext.logError(log, message);
            }

            if (LogWithCallContext.isLogAuditEnabled()) {
                String msg = String.format("Transaction fails to commit, num_inserts=%d, num_deletes=%d",
                        inserts.size(), deletions.size());
                LogWithCallContext.logAudit(msg);
            }

            //Note: even if the commit is retriable and the TemporaryBackendException is thrown here, at the commit(.)
            //method of StandardJanusGraph class, the thrown exception will be translated to rollback(.) and then
            //throw further JanusGraphException to the application. Thus, it is better to just throw the
            //PermanentBackendException here. as at this late commit stage, there is no retry logic defined
            //at the StandardJanusGraph class.
            //if (isRetriable)
            //    throw new TemporaryBackendException("Max transaction count exceed but transaction is retriable");
            //else
            //    throw new PermanentBackendException("Max transaction reset count exceeded");
            throw new PermanentBackendException("transaction fails to commit");
        }

        //the normal path
        FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                storeManager.metricGroup, opPerformed, isolationLevel, startTransactionTime, true);
        FoundationDBMetricsUtil.TransactionScope.incTransactions(
                storeManager.metricGroup, opPerformed, isolationLevel);
        FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                storeManager.metricGroup, opPerformed, isolationLevel);

    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }

    public byte[] get(final byte[] key) throws PermanentBackendException {
        if (log.isDebugEnabled()) {
            String message = "get-key comes into the normal flow with key: " + Arrays.toString(key)
                    + " thread id: " + Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);
        }

        long callStartTime = System.nanoTime();

        boolean failing = true;
        byte[] value = null;
        Exception lastException = null;
        for (int i = 0; i < maxRuns; i++) {
            try {
                value = this.tx.get(key).get();
                failing = false;
                break;
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "get encountered exception: " + e.getMessage());
                }

                FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                        storeManager.metricGroup,
                        callStartTime, FDB_TNX_METHOD_CALL.get, false);
                FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                        storeManager.metricGroup,
                        FDB_TNX_METHOD_CALL.get, false);

                handleFDBException(e);

                if (i+1 != maxRuns) {
                    this.restart();
                } else {
                    lastException = e;
                    break;
                }
            } catch (Exception e) {
                FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                        storeManager.metricGroup,
                        callStartTime, FDB_TNX_METHOD_CALL.get, false);
                FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                        storeManager.metricGroup, FDB_TNX_METHOD_CALL.get, false);

                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "get encountered exception: " + e.getMessage());
                }

                lastException = e;
                break;
            }
        }

        if (failing) {
            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                    storeManager.metricGroup,
                    callStartTime, FDB_TNX_METHOD_CALL.get, false);
            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                    storeManager.metricGroup, FDB_TNX_METHOD_CALL.get, false);

            throw new PermanentBackendException("FDB transaction throws an exception", lastException);
        }
        else {
            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                    storeManager.metricGroup,
                    callStartTime, FDB_TNX_METHOD_CALL.get, true);
            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallCounts(
                    storeManager.metricGroup, FDB_TNX_METHOD_CALL.get);
        }

        if (log.isDebugEnabled()) {
            long callEndStartTime = System.nanoTime();
            long total_time = callEndStartTime - callStartTime;
            String message = "get-key takes: "
                    + total_time / FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION + "(ms)"
                    + " thread id: " +  Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);

        }

        return value;
    }

    public List<KeyValue> getRange(final byte[] startKey, final byte[] endKey,
                                            final int limit) throws PermanentBackendException {
        long callStartTime = System.nanoTime();
        boolean failing = true;
        List<KeyValue> result = Collections.emptyList();
        Exception lastException = null;
        for (int i = 0; i < maxRuns; i++) {
            final int startTxId = txCtr.get();
            try {
                result = tx.getRange(new Range(startKey, endKey), limit).asList().get();
                if (result == null) return Collections.emptyList();
                failing = false;
                if (log.isDebugEnabled()) {
                    String message = "getRange comes into the normal flow with startkey: " +
                            Arrays.toString(startKey) + " endkey: "
                            + Arrays.toString(endKey) +
                            " limit: " + limit
                            + " thread id: " + Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId;
                    LogWithCallContext.logDebug(log, message);
                }

                break;
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log,"getRange encountered exception: " + e.getMessage());
                }

                FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                        storeManager.metricGroup,
                        callStartTime, FDB_TNX_METHOD_CALL.getRange, false);
                FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                        storeManager.metricGroup, FDB_TNX_METHOD_CALL.getRange, false);

                handleFDBException(e);

                if (txCtr.get() == startTxId) {
                    if (log.isDebugEnabled()) {
                        String message = "getRange tx counter is: " + txCtr.get() + " and assigned start tx id is: " + startTxId +
                                "  agree with each other" + "getRange receives exception: " + e.getMessage() + " before restart.";
                        LogWithCallContext.logDebug(log, message);
                    }

                    if (i+1 != maxRuns) {
                        this.restart();
                        if (log.isDebugEnabled()) {
                            String message = "transaction gets restarted, with iteration: " + i + " and maxruns: " + maxRuns;
                            LogWithCallContext.logDebug(log, message);
                        }
                    } else {
                        lastException = e;
                        break;
                    }
                }
                else {
                    if (log.isDebugEnabled()) {
                        String message = "getRange tx counter is: " + txCtr.get() + " and assigned start tx id is: " + startTxId +
                                " not agree with each other";
                        LogWithCallContext.logDebug(log, message);
                    }
                }
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    String message = "getRange encountered exception:" + e.getMessage();
                    LogWithCallContext.logError(log, message);
                }
                lastException = e;
                break;
            }
        }

        if (failing) {
            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                    storeManager.metricGroup,
                    callStartTime, FDB_TNX_METHOD_CALL.getRange, false);
            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                    storeManager.metricGroup, FDB_TNX_METHOD_CALL.getRange, false);
            if (log.isErrorEnabled()) {
                String message = "getRange has the final result with failing (should be true): " + failing;
                LogWithCallContext.logError(log, message);
            }

            //rely on the application-level retry mechanism for the graph transaction.
            throw new PermanentBackendException("FDB transaction throws an exception", lastException);
        } else {
            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                    storeManager.metricGroup,
                    callStartTime, FDB_TNX_METHOD_CALL.getRange, true);
            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallCounts(
                    storeManager.metricGroup, FDB_TNX_METHOD_CALL.getRange);
        }

        if (log.isDebugEnabled()) {
            long callEndTime = System.nanoTime();
            long totalTime = callEndTime - callStartTime;
            String message = "getRange takes " +
                    totalTime / FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION + "(ms)"
                    + " with result size: " + result.size()
                    + " with thread id: " + Thread.currentThread().getId() + " with transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);
        }
        return result;
    }

    public AsyncIterator<KeyValue> getRangeIter(final byte[] startKey, final byte[] endKey, final int limit) {
        return tx.getRange(new Range(startKey, endKey), limit, false, StreamingMode.WANT_ALL).iterator();
    }

    public AsyncIterator<KeyValue> getRangeIter(final byte[] startKey, final byte[] endKey, final int limit,
                                                final int skip) {
        // Avoid using KeySelector(byte[] key, boolean orEqual, int offset) directly as stated in KeySelector.java
        // that client code will not generally call this constructor.
        KeySelector begin = KeySelector.firstGreaterOrEqual(startKey).add(skip);
        KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
        return tx.getRange(begin, end, limit-skip, false, StreamingMode.WANT_ALL).iterator();
    }

    public synchronized  Map<KVQuery, List<KeyValue>> getMultiRange(final List<Object[]> queries)
            throws PermanentBackendException {
        if (log.isDebugEnabled()) {

            String message = "multi-range query, total number of queries to be put inside: " + queries.size()
                    + " thread id: " + Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);
        }

        long callStartTime = System.nanoTime();

        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        final List<Object[]> retries = new CopyOnWriteArrayList<>(queries);

        int counter = 0;
        for (int i = 0; i < maxRuns; i++) {
            counter++;

            if (retries.size() > 0) {

                List<Object[]> immutableRetries = retries.stream().collect(toList());

                final List<CompletableFuture<List<KeyValue>>> futures = new LinkedList<>();

                final int startTxId = txCtr.get();

                //Note: we introduce the immutable list for iteration purpose, rather than having the dynamic list
                //retries to be the iterator.
                for (Object[] obj : immutableRetries) {
                    final KVQuery query = (KVQuery) obj[0];
                    final byte[] start = (byte[]) obj[1];
                    final byte[] end = (byte[]) obj[2];

                    CompletableFuture<List<KeyValue>> f = tx.getRange(start, end, query.getLimit()).asList()
                        .whenComplete((res, th) -> {
                            if (th == null) {
                                if (log.isDebugEnabled()) {
                                    String message = "(before) get range succeeds with current size of retries: " + retries.size()
                                            + " thread id: " + Thread.currentThread().getId()
                                            + " transaction id: " + this.transactionId;
                                    LogWithCallContext.logDebug(log, message);
                                }

                                //Note: retries's object type is: Object[], not KVQuery.
                                retries.remove(obj);

                                if (log.isDebugEnabled()) {
                                    String message = "(after) get range succeeds with current size of retries: " + retries.size()
                                            + " thread id: " + Thread.currentThread().getId()
                                            + " transaction id: " + this.transactionId;
                                    LogWithCallContext.logDebug(log, message);
                                }

                                if (res == null) {
                                    res = Collections.emptyList();
                                }
                                resultMap.put(query, res);

                            } else {
                                Throwable t = th.getCause();
                                if (t != null && t instanceof FDBException) {
                                    FDBException fe = (FDBException) t;
                                    if (log.isDebugEnabled()) {
                                        String message = String.format(
                                                "Catch FDBException code=%s, isRetryable=%s, isMaybeCommitted=%s, "
                                                        + "isRetryableNotCommitted=%s, isSuccess=%s",
                                                fe.getCode(), fe.isRetryable(),
                                                fe.isMaybeCommitted(), fe.isRetryableNotCommitted(), fe.isSuccess());
                                        LogWithCallContext.logDebug(log, message);
                                    }
                                }

                                // Note: the restart here will bring the code into deadlock, as restart() is a
                                // synchronized method and the thread to invoke this method is from a worker thread
                                // that serves the completable future call, which is different from the thread that
                                // invokes the getMultiRange call (this method) and getMultiRange is also a synchronized
                                // call.
                                //if (startTxId == txCtr.get())
                                //    this.restart();
                                resultMap.put(query, Collections.emptyList());

                                if (log.isDebugEnabled()) {
                                    String message = "encounter exception with: " + th.getCause().getMessage();
                                    LogWithCallContext.logDebug(log, message);
                                }
                            }
                        });

                    futures.add(f);
                }

                CompletableFuture<Void> allFuturesDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                //when some of the Future encounters exception, map will ignore it. we need count as the action!
                //allFuturesDone.thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                try {
                    allFuturesDone.join();
                } catch (Exception ex) {
                    if (log.isErrorEnabled()) {
                        String message = "multi-range query encounters transient exception in some futures:" + ex.getCause().getMessage();
                        LogWithCallContext.logError(log, message);
                    }
                }

                if (log.isDebugEnabled()) {
                    String message = "get range succeeds with current size of retries: " + retries.size()
                            + " thread id: " + Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId;
                    LogWithCallContext.logDebug(log, message);
                }

                if (retries.size() > 0) {
                    if (log.isDebugEnabled()) {
                        String message = "in multi-range query, retries size: " + retries.size()
                                + " thread id: " + Thread.currentThread().getId()
                                + " transaction id: " + this.transactionId
                                + " start tx id: " + startTxId
                                + " txCtr id: " + txCtr.get();
                        LogWithCallContext.logDebug(log, message);
                    }
                    if (startTxId == txCtr.get()) {
                        if (log.isDebugEnabled()) {
                            String message = "in multi-range query, to go to restart"
                                    + " thread id: " + Thread.currentThread().getId()
                                    + " transaction id: " + this.transactionId;
                            LogWithCallContext.logDebug(log, message);
                        }
                        if (i+1 != maxRuns) {
                            this.restart();
                        }
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                    String message = "finish multi-range query's all of future-based query invocation with size: " + queries.size()
                            + " thread id: " + Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId
                            + " actual number of retries is: " + (counter - 1);
                    LogWithCallContext.logDebug(log, message);
                }
                break;
            }

        }

        if (retries.size() > 0) {
            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                    storeManager.metricGroup,
                    callStartTime, FDB_TNX_METHOD_CALL.getMultiRange, false);
            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                    storeManager.metricGroup,
                    FDB_TNX_METHOD_CALL.getMultiRange, false);

            if (log.isErrorEnabled()) {
                String message = "after max number of retries: " + maxRuns
                        + " some range queries still fail and forced with empty returns"
                        + " thread id: " + Thread.currentThread().getId()
                        + " transaction id: " + this.transactionId;
                LogWithCallContext.logError(log, message);
            }

            throw new PermanentBackendException("Encounter exceptions when invoking getRange(.) calls in getMultiRange(.)");
        }

        //the normal path
        FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                storeManager.metricGroup,
                callStartTime, FDB_TNX_METHOD_CALL.getMultiRange, true);
        FoundationDBMetricsUtil.FdbMethodScope.incMethodCallCounts(
                storeManager.metricGroup, FDB_TNX_METHOD_CALL.getMultiRange);

        if (log.isDebugEnabled()) {
            long callEndTime = System.nanoTime();
            long totalTime = callEndTime - callStartTime;

            int cnt = 0;
            for (List<KeyValue> vals: resultMap.values()) {
                cnt += vals.size();
            }
            String message = "multi-range takes" +
                    totalTime / FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION + "(ms)"
                    + " # queries: " + queries.size()
                    + " result size: " + cnt
                    + " thread id: " + Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);

        }

        return resultMap;
    }

    public void set(final byte[] key, final byte[] value) {
        inserts.add(new Insert(key, value));
        tx.set(key, value);
    }

    public void clear(final byte[] key) {
        deletions.add(key);
        tx.clear(key);
    }


    private class Insert {
        private byte[] key;
        private byte[] value;

        public Insert(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() { return this.key; }

        public byte[] getValue() { return this.value; }
    }

    private String determineReadWrite() {
        String operation = null;
        if (inserts.size() != 0 || deletions.size() != 0) {
            operation = "write";
        }
        else {
            operation = "read";
        }
        return operation;
    }
}
