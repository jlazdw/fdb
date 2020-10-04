// Copyright 2018 Expero Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsUtil;
import org.janusgraph.diskstorage.foundationdb.utils.LogWithCallContext;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

    private static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
        final byte[] bArray = new byte[limit - offset];
        System.arraycopy(array, offset, bArray, 0, limit - offset);
        return bArray;
    };


    private final DirectorySubspace db;
    private final String name;
    private final FoundationDBStoreManager manager;
    private boolean isOpen;

    public FoundationDBKeyValueStore(String n, DirectorySubspace data, FoundationDBStoreManager m) {
        db = data;
        name = n;
        manager = m;
        isOpen = true;
    }

     @Override
    public String getName() {
        return name;
    }

    private static FoundationDBTx getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        return ((FoundationDBTx) txh);//.getTransaction();
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            //if(isOpen) db.close();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        try {
            byte[] databaseKey = db.pack(key.as(ENTRY_FACTORY));
            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log, String.format("db=%s, op=get, tx=%s", name, txh));
            }

            final byte[] entry = tx.get(databaseKey);

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.get, true);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.get);

            if (entry != null) {
                return getBuffer(entry);
            } else {
                return null;
            }
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.get, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup, FDB_STORAGEPLUGIN_METHOD_CALL.get, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, "op=get exception", e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            if (log.isWarnEnabled()) {
                LogWithCallContext.logWarn(log, "Attempt to acquire lock with transactions disabled");
            }
        } //else we need no locking
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        if (manager.getMode() == FoundationDBStoreManager.NON_ASYNC) {
            return getSliceNonAsync(query, txh);
        } else {
            return getSliceAsync(query, txh);
        }
    }

    public RecordIterator<KeyValueEntry> getSliceNonAsync(KVQuery query, StoreTransaction txh) throws BackendException {
        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("beginning db=%s, op=getSlice, tx=%s", name, txh));
        }

        long callStartTime = System.nanoTime();

        final FoundationDBTx tx = getTransaction(txh);
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();
        final List<KeyValueEntry> result = new ArrayList<>();
        final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
        final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

        try {
            final List<KeyValue> results = tx.getRange(foundKey, endKey, query.getLimit());

            for (final KeyValue keyValue : results) {
                StaticBuffer key = getBuffer(db.unpack(keyValue.getKey()).getBytes(0));
                if (selector.include(key))
                    result.add(new KeyValueEntry(key, getBuffer(keyValue.getValue())));
            }
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, "op=getSliceNonAsync exception", e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }

        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("db=%s, op=getSlice, tx=%s, result-count=%d", name, txh, result.size()));
        }

        //normal path
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup,
                FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync);

        return new FoundationDBRecordIterator(result);
    }

    public RecordIterator<KeyValueEntry> getSliceAsync(KVQuery query, StoreTransaction txh) throws BackendException {
        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("beginning db=%s, op=getSlice, tx=%s", name, txh));
        }

        long callStartTime = System.nanoTime();

        final FoundationDBTx tx = getTransaction(txh);
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
        final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

        try {
            final AsyncIterator<KeyValue> result = tx.getRangeIter(foundKey, endKey, query.getLimit());

            //though this is an async. call, we still capture the latency overhead up to this point.
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync, true);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync);

            return new FoundationDBRecordIteratorForAsync(tx, foundKey, endKey, query.getLimit(), result,
                    query.getKeySelector());
        } catch (Exception e) {
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, String.format("getSliceAsync db=%s, tx=%s exception", name, txh), e);
            }

            throw new PermanentBackendException(e);
        }
    }

    private class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {
        private final Iterator<KeyValueEntry> entries;

        public FoundationDBRecordIterator(final List<KeyValueEntry> result) {
              this.entries = result.iterator();
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public KeyValueEntry next() {
            return entries.next();
        }

        @Override
        public void close() {
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class FoundationDBRecordIteratorForAsync implements RecordIterator<KeyValueEntry> {
        private final FoundationDBTx tx;
        private AsyncIterator<KeyValue> entries;
        private final KeySelector selector;
        KeyValue nextKeyValue = null;

        private final byte[] startKey;
        private final byte[] endKey;
        private final int limit;
        private int fetched;

        protected static final int TRANSACTION_TOO_OLD_CODE = 1007;

        public FoundationDBRecordIteratorForAsync(FoundationDBTx tx,
                                                  byte[] startKey, byte[] endKey,
                                                  int limit, final AsyncIterator<KeyValue> result,
                                                  KeySelector selector) {
            this.tx = tx;
            this.entries = result;
            this.selector = selector;
            this.startKey = startKey;
            this.endKey = endKey;
            this.limit = limit;
            this.fetched = 0;
        }

        @Override
        public boolean hasNext() {
            fetchNext();
            return (nextKeyValue != null);
        }

        @Override
        public KeyValueEntry next() {
            if (hasNext()) {
                KeyValue kv = nextKeyValue;
                nextKeyValue = null;
                StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                return new KeyValueEntry(key, getBuffer(kv.getValue()));
            } else {
                throw new IllegalStateException("Does not have any key-value to retrieve");
            }
        }

        private FDBException unwrapException(Throwable err) {
            Throwable curr = err;
            while (curr != null && !(curr instanceof FDBException))  {
                curr = curr.getCause();
            }
            if (curr != null) {
                return (FDBException) curr;
            } else {
                return null;
            }
        }

        private void fetchNext() {
            while (true) {
                try {
                    while (nextKeyValue == null && entries.hasNext()) {
                        KeyValue kv = entries.next();
                        fetched++;
                        StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                        if (selector.include(key)) {
                            nextKeyValue = kv;
                        }
                    }
                    break;
                } catch (RuntimeException e) {

                    log.info("Existing async iterator gets canceled.");
                    entries.cancel();

                    if (log.isErrorEnabled()) {
                        log.error("AsyncIterator fetchNext() throws exception", e);
                    }

                    Throwable t = e.getCause();
                    FDBException fdbException = unwrapException(t);
                    // Capture the transaction too old error
                    if (tx.getIsolationLevel() != FoundationDBTx.IsolationLevel.SERIALIZABLE &&
                            fdbException != null && (fdbException.getCode() == TRANSACTION_TOO_OLD_CODE)) {
                        tx.restart();
                        entries = tx.getRangeIter(startKey, endKey, limit, fetched);
                    } else {
                        if (log.isErrorEnabled()) {
                            log.error("The throwable is not restartable", t);
                        }
                        throw e;
                    }
                }
                catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        log.error("AsyncIterator fetchNext() throws exception", e);
                    }
                    throw e;
                }
            }
        }

        @Override
        public void close() {
            entries.cancel();
        }

        @Override
        public void remove() {
            entries.remove();
        }
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        if (manager.getMode() == FoundationDBStoreManager.NON_ASYNC) {
            return getSlicesNonAsync(queries, txh);
        } else {
            return getSlicesAsync(queries, txh);
        }
    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesNonAsync(List<KVQuery> queries, StoreTransaction txh) throws BackendException {

        //record the total number of sizes
        int totalNumberOfQueries = queries.size();
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordSizeOfMultiQueries(
                manager.metricGroup, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, totalNumberOfQueries);

        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log,
                    String.format("beginning db=%s, op=getSlices, tx=%s, in thread=%d", name, txh, Thread.currentThread().getId()));
        }

        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>();

        final List<Object[]> preppedQueries = new ArrayList<>(totalNumberOfQueries);
        for (final KVQuery query : queries) {
            final StaticBuffer keyStart = query.getStart();
            final StaticBuffer keyEnd = query.getEnd();
            final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
            final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));
            preppedQueries.add(new Object[]{query, foundKey, endKey});
        }

        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, "get slices invoking multi-range queries in thread = " + Thread.currentThread().getId());
        }

        try {
            final Map<KVQuery, List<KeyValue>> result = tx.getMultiRange(preppedQueries);

            for (Map.Entry<KVQuery, List<KeyValue>> entry : result.entrySet()) {
                final List<KeyValueEntry> results = new ArrayList<>();
                for (final KeyValue keyValue : entry.getValue()) {
                    final StaticBuffer key = getBuffer(db.unpack(keyValue.getKey()).getBytes(0));
                    if (entry.getKey().getKeySelector().include(key))
                        results.add(new KeyValueEntry(key, getBuffer(keyValue.getValue())));
                }
                resultMap.put(entry.getKey(), new FoundationDBRecordIterator(results));
            }
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, "op=getSlicesNonAsync exception", e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }

        //normal flow
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup,
                FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync);

        return resultMap;
    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesAsync(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        //record the total number of sizes
        int totalNumberOfQueries = queries.size();
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordSizeOfMultiQueries(
                manager.metricGroup, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, totalNumberOfQueries);


        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log,
                    String.format("beginning db=%s, op=getSlices, tx=%s, in thread=%d", name, txh, Thread.currentThread().getId()));
        }

        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>();

        try {
            for (final KVQuery query : queries) {
                final StaticBuffer keyStart = query.getStart();
                final StaticBuffer keyEnd = query.getEnd();
                final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
                final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

                // FIXME Is this operation expensive? Must be revisited if ASYNC is enabled.
                AsyncIterator<KeyValue> result = tx.getRangeIter(foundKey, endKey, query.getLimit());
                resultMap.put(query, new FoundationDBRecordIteratorForAsync(tx, foundKey, endKey,
                        query.getLimit(), result, query.getKeySelector()));
            }
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, String.format("getSlicesAsync db=%s, tx=%s exception", name, txh), e);
            }
            throw new PermanentBackendException(e);
        }

        //normal flow
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup, FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync);

        return resultMap;
    }


    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer integer) throws BackendException {
        insert(key, value, txh);
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        try {
            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log,String.format("db=%s, op=insert, tx=%s", name, txh));
            }
            tx.set(db.pack(key.as(ENTRY_FACTORY)), value.as(ENTRY_FACTORY));
        }
        catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.insert, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.insert, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, String.format("db=%s, op=insert, tx=%s throws exception", name, txh), e);
            }

            throw new PermanentBackendException(e);
        }

        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.insert, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup,
                FDB_STORAGEPLUGIN_METHOD_CALL.insert);
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        try {
            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log, String.format("db=%s, op=delete, tx=%s", name, txh));
            }

            tx.clear(db.pack(key.as(ENTRY_FACTORY)));
        }
        catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.delete, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FDB_STORAGEPLUGIN_METHOD_CALL.delete, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log,String.format("db=%s, op=delete, tx=%s throws exception", name, txh), e);
            }
            throw new PermanentBackendException(e);
        }

        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FDB_STORAGEPLUGIN_METHOD_CALL.delete, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup,
                FDB_STORAGEPLUGIN_METHOD_CALL.insert);
    }

    private static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
