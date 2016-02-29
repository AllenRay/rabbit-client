package  rabbit.lyra.config;

import  rabbit.lyra.event.ConnectionListener;
import com.rabbitmq.client.Connection;

import java.util.Collection;

/**
 * {@link Connection} related configuration.
 * 
 * @author Jonathan Halterman
 */
public interface ConnectionConfig extends ChannelConfig {
  /**
   * Returns the connection's listeners else empty list if none were configured.
   * 
   * @see #withConnectionListeners(ConnectionListener...)
   */
  Collection<ConnectionListener> getConnectionListeners();

  /**
   * Returns the connection's recovery policy.
   * 
   * @see #withConnectionRecoveryPolicy(RecoveryPolicy)
   */
  RecoveryPolicy getConnectionRecoveryPolicy();

  /**
   * Returns the connection's retry policy.
   * 
   * @see #withConnectionRetryPolicy(RetryPolicy)
   */
  RetryPolicy getConnectionRetryPolicy();

  /**
   * Sets the {@code connectionListeners} to call on connection related events.
   */
  ConnectionConfig withConnectionListeners(ConnectionListener... connectionListeners);

  /**
   * Sets the policy to use for the recovery of Connections after an unexpected Connection closure.
   */
  ConnectionConfig withConnectionRecoveryPolicy(RecoveryPolicy recoveryPolicy);

  /**
   * Sets the policy to use for handling {@link Connection} invocation errors.
   */
  ConnectionConfig withConnectionRetryPolicy(RetryPolicy retryPolicy);
}
