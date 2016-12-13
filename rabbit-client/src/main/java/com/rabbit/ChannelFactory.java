package com.rabbit;

import com.rabbit.exception.RabbitChannelException;
import com.rabbit.exception.RabbitConnectionException;
import com.rabbit.lyra.ConnectionOptions;
import com.rabbit.lyra.Connections;
import com.rabbit.lyra.config.Config;
import com.rabbit.lyra.config.RecoveryPolicies;
import com.rabbit.lyra.config.RetryPolicy;
import com.rabbit.lyra.util.Duration;
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
import java.util.Iterator;
import java.util.concurrent.*;

/**
 * Created by allen lei on 2015/12/4.
 *
 * 1,为了提高性能,producer,consumer 将会使用不同的connection。
 * 2,同时所有的producer 将会使用一个channel,而每个consumer 将会使用分离的channel
 *
 * producer 和 consumer 使用不同的connection来提高性能
 */
public class ChannelFactory {

    private final Logger logger = LoggerFactory.getLogger(ChannelFactory.class);

    private String address;

    private String host;

    private int port;

    private String userName;

    private String password;

    private String virtualHost;

    private ConnectionFactory factory;

    private Connection consumerConnection;

    private ExecutorService executorService;

    private int cacheSize = 1;

    private int connectionSize = 1;

    private BlockingQueue<Channel> producerChannels;

    private BlockingQueue<Connection> producerConections;


    /**
     * 初始化Producer Connection和Consumer Connection如果传入了connectionfactory，那么将使用传入的connectionFactory，
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
                            .withMaxDuration(Duration.minutes(5))).withConsumerRecovery(true).withQueueRecovery(true).withExchangeRecovery(true);
            ConnectionOptions connectionOptions = null;
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
                 connectionOptions = new ConnectionOptions().withAddresses(addresses).withUsername(getUserName()).withPassword(getPassword()).withVirtualHost(getVirtualHost());

            } else {
                connectionOptions = new ConnectionOptions().withHost(getHost()).withPort(getPort()).withUsername(getUserName()).withPassword(getPassword()).withVirtualHost(getVirtualHost());
            }
            if (getExecutorService() != null) {
                connectionOptions.withConsumerExecutor(getExecutorService());
            }
            //init producer connection
            producerConections = new LinkedBlockingQueue<>(getConnectionSize());
            for(int i = 0; i < getConnectionSize(); i++) {
                producerConections.offer(Connections.create(connectionOptions, config));
            }

            consumerConnection = Connections.create(connectionOptions, config);

            if (producerConections == null || producerConections.isEmpty()|| consumerConnection == null) {
                throw new RabbitConnectionException("Rabbit producerConnection has not instance,so abort it...");
            }

            //init producer channel
            //数量是缓存channel size 和 connection size
            int channelSize = getCacheSize()*getConnectionSize();
            producerChannels = new LinkedBlockingQueue<>(channelSize);
            for(int i = 0; i < channelSize; i++){
                producerChannels.offer(createProducerChannel());
            }

        } catch (IOException e) {
            throw new RabbitConnectionException("Init rabbit producerConnection occur io error.", e);
        } catch (TimeoutException e) {
            throw new RabbitConnectionException("Init rabbit producerConnection occur timeout error", e);
        }


    }

    /**
     * create a new producer channel.
     * @return
     */
    Channel createProducerChannel() {
        if (producerConections == null || producerConections.isEmpty()) {
            logger.error("Has no connection pools");
            throw new RabbitConnectionException("No available producerConnection.");
        }
        try {
            //从连接池head拿到连接然后创建channel
            //put connection to tail afte create channel
            Connection connection = producerConections.poll();
            Channel channel = connection.createChannel();
            producerConections.offer(connection);
            return channel;
        } catch (IOException e) {
            logger.error("Create channel occur error." + e);
            throw new RabbitChannelException("Create channel occur error." + e);
        }
    }

    /**
     * 从阻塞队列中拿到producer channel
     * 直接使用阻塞队列做同步，没必要使用lock或者信号量去做同步
     * @return
     */
    public Channel getProducerChannel(){
        long start = System.currentTimeMillis();
        try {
            Channel channel = producerChannels.poll(5,TimeUnit.MILLISECONDS);
            if(channel == null){
                return createProducerChannel();
            }
            return channel;
        } catch (InterruptedException e) {
            throw new RabbitChannelException("Cant get channel:"+e);
        }finally {
            long end = System.currentTimeMillis();
            logger.debug("Get channel spent {}ms",end-start);
        }
    }

    /**
     * 归还channel
     * @param channel
     */
    public void returnProducerChannel(Channel channel){
        if(channel != null && channel.isOpen()){
            boolean offerd = producerChannels.offer(channel);
            if(!offerd){
                try {
                    channel.close();
                    logger.debug("close channel when pool is full");
                } catch (IOException | TimeoutException e) {
                    logger.error("Close channel occur error:"+e);
                }
            }
        }
    }



    /**
     * create consumer channel
     * @return
     */
    public Channel createConsumerChannel(){
        if (consumerConnection == null || !consumerConnection.isOpen()) {
            logger.error("Connection is not opened.");
            throw new RabbitConnectionException("No available producerConnection.");
        }
        try {
            return consumerConnection.createChannel();
        } catch (IOException e) {
            logger.error("Create channel occur error." + e);
            throw new RabbitChannelException("Create channel occur error." + e);
        }
    }


    /**
     * close channel.
     *
     * @param channel
     */
    void closeChannel(Channel channel) {
        if (channel == null || !channel.isOpen()) {
            logger.warn("Channel is closed.");
            return;
        }
        try {
            channel.close();
        } catch (IOException | TimeoutException e) {
            logger.error("Close channel occur error." + e);
        }
    }


    /**
     * 不需要显示的关闭所有channel，如果connection关闭了，将会自动关闭这个connection上面的所有channel
     */
    @PreDestroy
    public void closeConnection() {
        try {
            if (producerConections != null && !producerConections.isEmpty()) {
                for (Iterator<Connection> it = producerConections.iterator(); it.hasNext();){
                    Connection connection = it.next();
                    connection.close();
                }
            }

            if (consumerConnection != null) {
                consumerConnection.close();
            }
        } catch (IOException e) {
            throw new RabbitConnectionException("Close rabbit producerConnection failed ", e);
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

    public int getConnectionSize() {
        return connectionSize;
    }

    public void setConnectionSize(int connectionSize) {
        this.connectionSize = connectionSize;
    }
}
