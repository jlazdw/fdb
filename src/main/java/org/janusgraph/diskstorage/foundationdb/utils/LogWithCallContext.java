package org.janusgraph.diskstorage.foundationdb.utils;


import org.janusgraph.diskstorage.tracing.CallContext;
import org.janusgraph.diskstorage.tracing.CallCtxThreadLocalHolder;

import org.slf4j.Logger;

/**
 * To introduce call context aware logging, with call context stored in thread local storage. Note that only when the
 * caller explicitly sets the context, will the storage plugin code pick it up. Some JanusGraph background threads do not
 * introduce call context.
 *
 * The log checking: log.isDebugEnabled(), isErrorEnabled(), and isWarnEnabled(), now get pushed to the caller, as the caller can
 * save the String formatting cost, by having the checking performed at the caller side.
 */
public class LogWithCallContext {

    /**
     * to log the debug message with call context information.
     * @param logger the logger created by the immediate caller
     * @param message the debug message.
     */
    public static void logDebug (Logger logger, String message) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        if (context == null) {
            logger.debug(message);
        }
        else {
            String contextAttached =
                    String.format(message + " with context { " +
                                    " operation: [%s]" + " keyspace: [%s] "
                                    + " request id: [%s]" + " app id: [%s] "
                                    + " client address: [%s]}",
                            context.getOperationName(),
                            context.getKeyspaceName(),
                            context.getRequestId(),
                            context.getApplicationId(),
                            context.getClientAddress());

            logger.debug(contextAttached);
        }
    }

    public static boolean isLogAuditEnabled() {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        return context == null ? false : (context.getAuditLogger() != null);
    }

    public static void logAudit(String message) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        if (context != null) {
            String contextAttached = String.format(
                    "Keyspace=%s, RequestId=%s, GremlinRequestId=%s, AppId=%s, ClientAddress=%s: %s",
                    context.getKeyspaceName(),
                    context.getRequestId(),
                    context.getGremlinRequestId(),
                    context.getApplicationId(),
                    context.getClientAddress(),
                    message);

            if (context.getAuditLogger() != null) {
                context.getAuditLogger().info(contextAttached);
            }
        }
    }


    /**
     * to log the debug message with call context information
     * @param logger the logger created by the immediate caller
     * @param message the immediate caller passed-in message
     * @param ex the exception captured by the the immediate caller
     */
    public static void logDebug (Logger logger, String message, Throwable ex) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();

        if (context == null) {
            logger.debug(message, ex);
        }
        else {
            String contextAttached =
                    String.format(message + " with context { " +
                                    " operation: [%s]" + " keyspace: [%s] "
                                    + " request id: [%s]" + " app id: [%s] "
                                    + " client address: [%s]}",
                            context.getOperationName(),
                            context.getKeyspaceName(),
                            context.getRequestId(),
                            context.getApplicationId(),
                            context.getClientAddress());

            logger.debug(contextAttached, ex);

        }

    }


    /**
     * to log the error message with call context information
     *
     * @param logger the logger created by the immediate caller
     * @param message the immediate caller passed-in message
     */
    public static void logError (Logger logger, String message) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        if (context == null) {
            logger.error (message);
        }
        else {
            String contextAttached =
                    String.format(message + " with context {" +
                                    " operation: [%s]" + " keyspace: [%s] "
                                    + " request id: [%s]" + " app id: [%s] "
                                    + " client address: [%s]}",
                            context.getOperationName(),
                            context.getKeyspaceName(),
                            context.getRequestId(),
                            context.getApplicationId(),
                            context.getClientAddress());

            logger.error (contextAttached);
        }

    }

    /**
     * to log the error with call context information
     * @param logger the logger create by the immediate caller
     * @param message the immediate caller passed-in message
     * @param ex the exception captured by the the immediate caller
     */
    public static void logError (Logger logger, String message, Throwable ex) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        if (context == null) {
            logger.error(message, ex);
        }
        else {
            String contextAttached =
                    String.format(message + " with context { " +
                                    " operation: [%s]" + " keyspace: [%s] "
                                    + " request id: [%s]" + " app id: [%s] "
                                    + " client address: [%s]}",
                            context.getOperationName(),
                            context.getKeyspaceName(),
                            context.getRequestId(),
                            context.getApplicationId(),
                            context.getClientAddress());

            logger.error(contextAttached, ex);
        }
    }


    /**
     * to log the warning message with call context information
     *
     * @param logger the logger created by the immediate caller
     * @param message the immediate caller passed-in message
     */
    public static void logWarn (Logger logger, String message) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        if (context == null) {
            logger.warn (message);
        }
        else {
            String contextAttached =
                    String.format(message + " with context {" +
                                    " operation: [%s]" + " keyspace: [%s] "
                                    + " request id: [%s]" + " app id: [%s] "
                                    + " client address: [%s]}",
                            context.getOperationName(),
                            context.getKeyspaceName(),
                            context.getRequestId(),
                            context.getApplicationId(),
                            context.getClientAddress());

            logger.warn (contextAttached);
        }
    }


    /**
     * to log the warning with call context information
     * @param logger the logger created by the immediate caller
     * @param message the immediate caller passed-in message
     * @param ex the exception captured by the the immediate caller
     */
    public static void logWarn (Logger logger, String message, Throwable ex) {
        CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
        if (context == null) {
            logger.warn(message, ex);
        } else {
            String contextAttached =
                    String.format(message + " with context { " +
                                    " operation: [%s]" + " keyspace: [%s] "
                                    + " request id: [%s]" + " app id: [%s] "
                                    + " client address: [%s]}",
                            context.getOperationName(),
                            context.getKeyspaceName(),
                            context.getRequestId(),
                            context.getApplicationId(),
                            context.getClientAddress());

            logger.warn(contextAttached, ex);

        }
    }
}
