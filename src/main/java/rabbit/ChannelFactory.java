package rabbit;

import rabbit.exception.RabbitChannelException;
import rabbit.exception.RabbitConnectionException;
import rabbit.lyra.ConnectionOptions;
import rabbit.lyra.Connections;
import rabbit.lyra.config.Config;
import rabbit.lyra.config.RecoveryPolicies;
import rabbit.lyra.config.RetryPolicy;
import rabbit.lyra.util.Duration;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by allen lei on 2015/12/4.
 * 使用一个connection 管理多个channel。
 */
public class ChannelFactory {

    private final Logger logger = LoggerFactory.getLogger(rabbit.ChannelFactory.class);

    private final Lock lock = new ReentrantLock();

    private Queue<Channel> channelQueue;

    private String address;

    private String host;

    private int port;

    private String userName;

    private String password;

    private String virtualHost;

    private boolean autoRecovery = true;

    private ConnectionFactory factory;

    private Connection connection;

    private ExecutorService executorService;

    private int cacheSize;


    /**
     * 初始化connection，如果传入了connectionfactory，那么将使用传入的connectionFactory，
     * 如果没有就构造一个默认的connectionFactory，然后根据注入的设置一些属性。
     */
    @PostConstruct
    public void initConnection() {

        try {
            Config config = new Config()
                    .withRecoveryPolicy(RecoveryPolicies.recoverAlways())
                    .withRetryPolicy(new RetryPolicy()
                            .withMaxAttempts(10)
                            .withInterval(Duration.seconds(5))
                            .withMaxDuration(Duration.minutes(5))).withConsumerRecovery(true);

            if (!StringUtils.isEmpty(getAddress())) {
                String[] adds = getAddress().split(",");
                if (adds == null || adds.length == 0) {
                    throw new RabbitConnectionException("Connect address is empty.");
                }
                Address[] addresses = new Address[adds.length];
                for (int i = 0; i < adds.length; i++) {
                    String connectAdd = adds[0];
                    if (connectAdd.indexOf(":") > 0) {
                        String[] hostAndPort = connectAdd.split(":");
                        addresses[i] = new Address(hostAndPort[0], Integer.valueOf(hostAndPort[1]));
                    } else {
                        addresses[i] = new Address(adds[0]);
                    }
                }
                ConnectionOptions connectionOptions = new ConnectionOptions().withAddresses(addresses).withUsername(getUserName()).withPassword(getPassword()).withVirtualHost(getVirtualHost());

                if (getExecutorService() != null) {
                    connectionOptions.withConsumerExecutor(getExecutorService());
                }
                connection = Connections.create(connectionOptions, config);
            } else {
                ConnectionOptions connectionOptions = new ConnectionOptions().withHost(getHost()).withPort(getPort()).withUsername(getUserName()).withPassword(getPassword()).withVirtualHost(getVirtualHost());

                if (getExecutorService() != null) {
                    connectionOptions.withConsumerExecutor(getExecutorService());
                }

                connection = Connections.create(connectionOptions, config);
            }

            //缓存的长度
            if (getCacheSize() > 0) {
                channelQueue = new ArrayBlockingQueue<>(getCacheSize());
            }

        } catch (IOException e) {
            logger.error("Init  rabbit connection occur error {}", e);
            throw new RabbitConnectionException("Init  rabbit connection occur io error,the error is: " + e);
        } catch (TimeoutException e) {
            logger.error("Init  rabbit connection occur error {}", e);
            throw new RabbitConnectionException("Init  rabbit connection occur timeout error,the error is: " + e);
        }

        if (connection == null) {
            logger.error("Create connection occur error.");
            throw new RabbitConnectionException("Rabbit connection has not instance,so abort it...");
        }

    }

    /**
     * 获取新的channel
     *
     * @return 获取channel，如果缓存里面有，就先从缓存里面获取。如果没有则创建新的。
     */
    public Channel getChannel() {
        if (connection == null) {
            logger.error("No available  rabbit connection");
            throw new RabbitConnectionException("No available  rabbit connection.");
        }
        try {
            lock.lock();
            if (channelQueue != null && !channelQueue.isEmpty()) {
                Channel channel = channelQueue.poll();
                if (!channel.isOpen()) {
                    return getChannel();
                }
                return channel;
            }
            return createNewChannel();
        } finally {
            lock.unlock();
        }

    }

    private Channel createNewChannel() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("create a new  rabbit channel....");
            }
            final Channel channel = connection.createChannel();
            return channel;
        } catch (IOException e) {
            logger.error("Create channel failed,the reason is: {}", e);
            throw new RabbitChannelException("Create channel failed,the reason is: " + e);
        }
    }

    /**
     * 关闭channel
     *
     * @param channel
     */
    public void closeChannel(Channel channel) {
        try {
            lock.lock();
            if (channel != null && channel.isOpen()) {
                if (channelQueue != null) {
                    boolean successful = channelQueue.offer(channel);
                    if (!successful) {
                        channel.close();
                    }
                } else {
                    channel.close();
                }
            }
        } catch (IOException e) {
            logger.error("close channel occur io error,the error is : {}", e);
            throw new RabbitChannelException("close channel occur io error,the error is : " + e);
        } catch (TimeoutException e) {
            logger.error("close channel time out,the error is : {}", e);
            throw new RabbitChannelException("close channel time out,the error is : " + e);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 不需要显示的关闭所有channel，如果connection关闭了，将会自动关闭这个connection上面的所有channel
     */
    @PreDestroy
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("Close  rabbit connection failed,the error is: {}", e);
                throw new RabbitConnectionException("Close  rabbit connection failed,the error is: " + e);
            }
        }


    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAutoRecovery() {
        return autoRecovery;
    }

    public void setAutoRecovery(boolean autoRecovery) {
        this.autoRecovery = autoRecovery;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public ConnectionFactory getFactory() {
        return factory;
    }

    public void setFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public Queue<Channel> getChannelQueue() {
        return this.channelQueue;
    }

    public void setChannelQueue(Queue<Channel> channelQueue) {
        this.channelQueue = channelQueue;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
