package org.janusgraph.diskstorage.tracing;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Common Thread Local Storage Holder used by NuGraph Service and NuGraph Storage Plugin, which are two libraries
 * co-located in the same NuGraph Service process.
 *
 */
public class CallCtxThreadLocalHolder {
    public static FastThreadLocal<CallContext> callCtxThreadLocal = new FastThreadLocal<>();
}
