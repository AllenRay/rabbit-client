package com.rabbit.lyra.internal.util;

import com.rabbitmq.client.*;

import java.io.EOFException;
import java.util.Set;

public final class Exceptions {
  private Exceptions() {}

  @SuppressWarnings("unchecked")
  public static <T extends Throwable> T extractCause(Throwable t, Class<T> type) {
    Throwable cause = t;
    while (cause != null) {
      if (type.isAssignableFrom(cause.getClass()))
        return (T) cause;
      cause = cause.getCause();
    }

    return null;
  }

  public static boolean isCausedByConnectionClosure(Exception e) {
    ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
    return sse != null && Exceptions.isConnectionClosure(sse);
  }

  /**
   * Reliably returns whether the shutdown signal represents a connection closure.
   */
  public static boolean isConnectionClosure(ShutdownSignalException e) {
    return e instanceof AlreadyClosedException ? e.getReference() instanceof Connection
        : e.isHardError();
  }

  public static boolean isRetryable(Set<Class<? extends Exception>> retryableExceptions,
      Exception e, ShutdownSignalException sse) {
    for (Class<? extends Exception> retryable : retryableExceptions)
      if (retryable.isAssignableFrom(e.getClass()))
        return true;
    if (retryableExceptions.contains(e) || e.getCause() instanceof EOFException)
      return true;
    if (e instanceof PossibleAuthenticationFailureException)
      return false;
    return sse != null && isRetryable(sse);
  }

  private static boolean isRetryable(int failureCode) {
    switch (failureCode) {
    /** Channel failures */
      case 311: // Content too large
        return true;
      case 313: // No consumers
        return false;
      case 403: // Access refused
        return false;
      case 404: // Not found
        return false;
      case 405: // Resource locked
        return false;
      case 406: // Precondition failed
        return false;

        /** Connection failures */
      case 320: // Connection forced
        return true;
      case 402: // Invalid path
        return false;
      case 501: // Frame error
        return false;
      case 502: // Syntax error
        return false;
      case 503: // Invalid Command
        return false;
      case 504: // Channel error
        return false;
      case 505: // Unexpected frame
        return false;
      case 506: // Resource error
        return false;
      case 530: // Not allowed
        return false;
      case 540: // Not implemented
        return false;
      case 541: // Internal error
        return true;

      default:
        return false;
    }
  }

  private static boolean isRetryable(ShutdownSignalException e) {
    if (e.isInitiatedByApplication())
      return false;
    Method method = e.getReason();
    if (method instanceof AMQP.Connection.Close)
      return isRetryable(((AMQP.Connection.Close) method).getReplyCode());
    if (method instanceof AMQP.Channel.Close)
      return isRetryable(((AMQP.Channel.Close) method).getReplyCode());
    return false;
  }
}
