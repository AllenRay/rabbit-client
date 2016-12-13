package com.rabbit;

import com.rabbit.common.LocalhostService;
import com.rabbit.consumer.OneplusRetryMessageConsumer;
import com.rabbit.consumer.OneplusSimpleMessageConsumer;
import com.rabbit.exception.RabbitChannelException;
import com.rabbit.handler.HandlerService;
import com.rabbit.lyra.internal.util.Assert;
import com.rabbit.lyra.internal.util.concurrent.NamedThreadFactory;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbit.producer.*;
import com.rabbit.producer.compensation.*;
import com.rabbit.store.*;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ShardedJedisPool;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by allen lei on 2015/12/7.
 * Rabbit template,提供send 和 receive
 * <p/>
 * 提供封装之后的producer和consumer的操作
 * 1，producer和consumer采用分离从connection，从而提高性能，做到互相不影响。
 * 2，producer将会使用channel池来发送消息，在配置channel factory的时候，决定池的大小。
 * 3，每个consumer将会使用单独的channel来接收消息。
 */
public class RabbitTemplate {

    private Logger logger = LoggerFactory.getLogger("#rabbitLog#");

    private ChannelFactory channelFactory;

    private MessageConverter messageConverter;

    private HandlerService handlerService;

    private String queue;

    private String exchange;

    private String routingKey = "";

    private ApplicationContext context;

    private StringValueResolver stringValueResolver;

    private Producer messageProducer;

    private Producer confirmMessageProducer;

    private CompensateProducer compensateProducer;

    private CompensateProducer redisCompensateProducer;

    private MessageStoreAndRetriever messageStoreAndRetriever;

    private boolean needCompensation = false;

    private String appName;

    private String zkConnect;

    private int coreSize = 0;

    private int maxSize = 0;

    private final static int DEFAULT_CORE_SIZE = 5;

    private final static int DEFAULT_MAX_SIZE = 10;

    private ThreadPoolExecutor executor;

    private JedisPool jedisPool;

    private ShardedJedisPool shardedJedisPool;

    private LocalhostService localhostService = new LocalhostService();

    /**
     * 发送消息的low level API，
     * 将只会处理消息的序列化和channel的获取和关闭。
     * 客户端程序在拿到channel实现自己的业务逻辑。
     *
     * @param message
     * @param messageChannelCallback
     */
    @Deprecated
    public void sendMessage(Message message, MessageChannelCallback messageChannelCallback) {
        Channel channel = channelFactory.createProducerChannel();
        try {
            byte[] messages = getMessageConverter().convertToMessage(message);
            messageChannelCallback.handleChannel(messages, channel);
        } finally {
            channelFactory.closeChannel(channel);
        }
    }

    /**
     * @param message
     */
    public void sendMessage(Message message) {
        sendMessage(message, getExchange(), getRoutingKey(), null);
    }

    public void sendConfirmMessage(Message message) {
        sendConfirmMessage(message, getExchange(), getRoutingKey(), null);
    }


    /**
     * @param message
     * @param exchange
     * @param routingKey
     */
    public void sendMessage(Message message, String exchange, String routingKey) {
        sendMessage(message, exchange, routingKey, null);
    }

    public void sendConfirmMessage(Message message, String exchange, String routingKey) {
        sendConfirmMessage(message, exchange, routingKey, null);
    }


    /**
     * 适用于fanout的exchange
     *
     * @param message
     * @param exchange
     */
    public void sendMessage(Message message, String exchange) {
        sendMessage(message, exchange, "");
    }

    /**
     * 适用于fanout的exchange
     *
     * @param message
     * @param exchange
     */
    public void sendConfirmMessage(Message message, String exchange) {
        sendConfirmMessage(message, exchange, "");
    }


    /**
     * @param message
     * @param exchange
     * @param routingKey      特别注意routing key 如果你使用fanout，那么routing key 可以传空字符。但是记住别传null
     * @param basicProperties
     */
    public void sendMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(message);
        Assert.notNull(exchange);

        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                messageProducer.sendMessage(exchange, routingKey, message, basicProperties, channel);
            }
        });
    }

    /**
     * send confirm message.
     *
     * @param message
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendConfirmMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(message);
        Assert.notNull(exchange);

        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                confirmMessageProducer.sendMessage(exchange, routingKey, message, basicProperties, channel);
            }
        });
    }


    /**
     * 发送持久化的消息，rabbit默认是当消息在内存中堆积到一定程度后，才会刷盘
     * 这个方法是立即刷盘，注意使用，会影响性能
     *
     * @param message
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendPersistenceMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(message);
        Assert.notNull(routingKey);

        if (basicProperties != null) {
            final AMQP.BasicProperties pros = basicProperties.builder().deliveryMode(2).build();
            sendMessage(message, exchange, routingKey, pros);
        } else {
            sendMessage(message, exchange, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC);
        }
    }

    public void sendConfirmPersistenceMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(message);
        Assert.notNull(routingKey);

        if (basicProperties != null) {
            final AMQP.BasicProperties pros = basicProperties.builder().deliveryMode(2).build();
            sendConfirmMessage(message, exchange, routingKey, pros);
        } else {
            sendConfirmMessage(message, exchange, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC);
        }
    }


    /**
     * @param message
     * @param exchange
     * @param routingKey
     */
    public void sendPersistenceMessage(final Message message, final String exchange, final String routingKey) {
        this.sendPersistenceMessage(message, exchange, routingKey, null);
    }

    public void sendConfirmPersistenceMessage(final Message message, final String exchange, final String routingKey) {
        this.sendConfirmPersistenceMessage(message, exchange, routingKey, null);
    }

    /**
     * 异步发送持久化的消息
     *
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendAsyncPersistenceMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        if (basicProperties != null) {
            final AMQP.BasicProperties pros = basicProperties.builder().deliveryMode(2).build();
            sendAsyncMessage(message, exchange, routingKey, pros);
        } else {
            sendAsyncMessage(message, exchange, routingKey, MessageProperties.MINIMAL_PERSISTENT_BASIC);
        }
    }


    public void sendAsyncPersistenceMessage(final Message message, final String exchange, final String routingKey) {
        sendAsyncPersistenceMessage(message, exchange, routingKey, null);
    }


    /**
     * @param message
     * @param exchange
     * @param routingKey
     */
    public void sendAsyncMessage(final Message message, final String exchange, final String routingKey) {
        this.sendAsyncMessage(message, exchange, routingKey, null);
    }

    public void sendConfirmAsyncMessage(final Message message, final String exchange, final String routingKey) {
        this.sendConfirmAsyncMessage(message, exchange, routingKey, null);
    }

    /**
     * @param message
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendAsyncMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(message);
        Assert.notNull(exchange);

        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                messageProducer.sendAsyncMessage(exchange, routingKey, message, basicProperties, channel);
            }
        });
    }


    public void sendConfirmAsyncMessage(final Message message, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(message);
        Assert.notNull(exchange);

        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                confirmMessageProducer.sendAsyncMessage(exchange, routingKey, message, basicProperties, channel);
            }
        });
    }


    /**
     * @param messageSenderCallback
     */
    private void send(MessageSenderCallback messageSenderCallback) {
        Channel channel = channelFactory.getProducerChannel();
        try {
            messageSenderCallback.execute(channel);
        } finally {
            channel.clearConfirmListeners();
            channelFactory.returnProducerChannel(channel);
        }

    }


    /**
     * 使用一个channel 发送多个message。
     * handler得到的只是deliveryTag以及ACK是否确认
     *
     * @param messages
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendMultiMessage(final List<Message> messages, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(messages);
        Assert.notNull(exchange);

        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                messageProducer.batchSendMessage(exchange, routingKey, messages, basicProperties, channel);
            }
        });
    }

    /**
     * 绝对不要实现ACK 之后的handler。
     *
     * @param messages
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendConfirmMultiMessage(final List<Message> messages, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(messages);
        Assert.notNull(exchange);

        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                confirmMessageProducer.batchSendMessage(exchange, routingKey, messages, basicProperties, channel);
            }
        });
    }

    public void sendMultiAsyncMessage(final List<Message> messages, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(messages);
        Assert.notNull(exchange);


        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                messageProducer.batchSendAsyncMessages(exchange, routingKey, messages, basicProperties, channel);
            }
        });
    }

    public void sendConfirmMultiAsyncMessage(final List<Message> messages, final String exchange, final String routingKey, final AMQP.BasicProperties basicProperties) {
        Assert.notNull(messages);
        Assert.notNull(exchange);


        send(new MessageSenderCallback() {
            @Override
            public void execute(Channel channel) {
                confirmMessageProducer.batchSendAsyncMessages(exchange, routingKey, messages, basicProperties, channel);
            }
        });
    }

    /**
     * @param messages
     * @param exchange
     * @param routingKey
     */
    public void sendMultiMessage(List<Message> messages, String exchange, String routingKey) {
        sendMultiMessage(messages, exchange, routingKey, null);
    }

    public void sendConfirmMultiMessage(List<Message> messages, String exchange, String routingKey) {
        sendConfirmMultiMessage(messages, exchange, routingKey, null);
    }

    /**
     * fanout exchange
     *
     * @param messages
     * @param exchange
     */
    public void sendMultiMessage(List<Message> messages, String exchange) {
        sendMultiMessage(messages, exchange, "", null);
    }

    public void sendConfirmMultiMessage(List<Message> messages, String exchange) {
        sendConfirmMultiMessage(messages, exchange, "", null);
    }

    /**
     * @param messages
     */
    public void sendMultiMessage(List<Message> messages) {
        sendMultiMessage(messages, getExchange(), getRoutingKey(), null);
    }

    public void sendConfirmMultiMessage(List<Message> messages) {
        sendConfirmMultiMessage(messages, getExchange(), getRoutingKey(), null);
    }

    /**
     * 发送需要补偿的消息
     * 采用redis实现补偿功能
     * @param message
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendCompensationMessage(Message message, String exchange, String routingKey, AMQP.BasicProperties basicProperties) {
        if (!isNeedCompensation()) {
            logger.error("The needCompensation must be set to true.");
            return;
        }
        CompensationMessage compensationMessage = new CompensationMessage();
        compensationMessage.setBasicProperties(basicProperties);
        compensationMessage.setExchangeKey(exchange);
        compensationMessage.setRoutingKey(routingKey);
        compensationMessage.setMessage(message);
        redisCompensateProducer.sendCompensationMessage(compensationMessage);
    }

    /**
     * 发送补偿消息，采用数据库，实现补偿功能
     * @param message
     * @param exchange
     * @param routingKey
     * @param messageId
     * @param basicProperties
     */
    public void sendCompensationMessage(Message message, String exchange, String routingKey, int messageId, AMQP.BasicProperties basicProperties){
        if (!isNeedCompensation()) {
            logger.error("The needCompensation must be set to true.");
            return;
        }

        CompensationMessage compensationMessage = new CompensationMessage();
        compensationMessage.setBasicProperties(basicProperties);
        compensationMessage.setExchangeKey(exchange);
        compensationMessage.setRoutingKey(routingKey);
        compensationMessage.setMessage(message);
        compensationMessage.setMessageId(messageId);
        compensateProducer.sendCompensationMessage(compensationMessage);
    }

    /**
     * store message.
     * 不要使用此方法
     *
     * @param message
     * @param exchange
     * @param routingKey
     * @param messageKey
     * @param basicProperties
     * @return
     */
    @Deprecated
    public void storeMessage(Message message, String exchange, String routingKey, String messageKey, AMQP.BasicProperties basicProperties) {
        MessageStoreBean messageStoreBean = new MessageStoreBean();
        messageStoreBean.setExchange(exchange);
        messageStoreBean.setRoutingKey(routingKey);
        messageStoreBean.setMessageKey(StringUtils.isEmpty(messageKey) ? message.getMessageId() : messageKey);
        messageStoreBean.setBasicProperties(basicProperties);
        messageStoreBean.setPayload(this.messageConverter.convertToMessage(message));
        this.messageStoreAndRetriever.storeMessage(messageStoreBean);
    }

    /**
     * store message.
     * @param message
     * @param exchange
     * @param routingKey
     * @param basicProperties
     * @return
     */
    public Integer storeMessage(Message message, String exchange, String routingKey, AMQP.BasicProperties basicProperties){
        MessageStoreBean messageStoreBean = new MessageStoreBean();
        messageStoreBean.setExchange(exchange);
        messageStoreBean.setRoutingKey(routingKey);
        messageStoreBean.setBasicProperties(basicProperties);
        messageStoreBean.setMessageKey(UUID.randomUUID().toString());
        messageStoreBean.setPayload(this.messageConverter.convertToMessage(message));
        return this.messageStoreAndRetriever.storeMessage(messageStoreBean);

    }

    /*
    * @param queue
    * @param autoACK is auto ack
    * @param fetchCount
    */
    public void receive(final String queue, boolean autoACK, int fetchCount) {
        final Channel channel = channelFactory.createConsumerChannel();
        try {
            consuming(queue, autoACK, fetchCount, channel);
        } catch (Exception e) {
            //will close channel if occur exception,like NullPointException and others.
            //so we need recovery channel after exception occured.
            throw new RabbitChannelException("Receive message failed ", e);
        }
    }

    /**
     * @param queue
     * @param fetchCount 每次消费多少条消息
     *                   <p/>
     *                   auto ack is false.
     */
    public void receive(final String queue, int fetchCount) {
        receive(queue, false, fetchCount);
    }

    /**
     * consuming message.
     *
     * @param queue
     * @param fetchCount
     * @param channel
     * @throws IOException
     */
    private void consuming(final String queue, boolean autoACK, int fetchCount, final Channel channel) throws IOException {
        channel.basicQos(fetchCount);
        channel.basicConsume(queue, false, getConsumerTag(),new OneplusSimpleMessageConsumer(channel, handlerService, getMessageConverter(), queue,executor));
    }

    /**
     * get consumer tag.
     * @return
     */
    private String getConsumerTag(){
        return "OnePlus:"+getAppName()+":"+localhostService.getIp();
    }

    /**
     * use spring retry to support.
     * currently,the retry logic is hard code,
     * will retry three times.
     *
     * @param queue
     * @param autoACK    is auto ack
     * @param fetchCount
     */
    public void receiveMessageWithRetry(final String queue, boolean autoACK, int fetchCount) {
        final Channel channel = channelFactory.createConsumerChannel();
        try {
            //consuming(queue, fetchCount, channel);
            channel.basicQos(fetchCount);
            channel.basicConsume(queue, autoACK, getConsumerTag(),new OneplusRetryMessageConsumer(channel, this.handlerService, messageConverter, queue,executor));
        } catch (Exception e) {
            //will close channel if occur exception,like NullPointException and others.
            //so we need recovery channel after exception occured.
            throw new RabbitChannelException("Receive message failed.", e);

        }
    }

    /**
     * auto ack is false.
     *
     * @param queue
     * @param fetchCount
     */
    public void receiveMessageWithRetry(final String queue, int fetchCount) {
        this.receiveMessageWithRetry(queue, false, fetchCount);
    }

    /**
     * auto ack is false
     *
     * @param queue
     */
    public void receiveMessageWithRetry(final String queue) {
        this.receiveMessageWithRetry(queue, 1);
    }

    /**
     * 默认每次消费一条
     *
     * @param queue
     */
    public void receive(String queue) {
        receive(queue, 1);
    }

    /**
     * 默认fetchcount是1，默认每次consume一条消息
     */
    public void receive() {
        receive(getQueue(), 1);
    }


    @PostConstruct
    public void start() {
        Assert.notNull(channelFactory);
        Assert.notNull(getMessageConverter());
        Assert.notNull(handlerService);
        Assert.notNull(appName,"The app name cant be null");

        //init default mq messageProducer.
        if (messageProducer == null) {
            this.messageProducer = new DefaultMessageProducer(getMessageConverter(), handlerService);
        }

        //init confirm message producer.
        if (confirmMessageProducer == null) {
            confirmMessageProducer = new ConfirmMessageProducer(getMessageConverter(), handlerService);
        }
        //is need message compenstate.
        if (isNeedCompensation()) {
            Assert.notNull(getJedisPool());
            Assert.notNull(getShardedJedisPool());

            CompensateManager compensateManager = new CompensateManager(getChannelFactory(),getJedisPool(),getMessageConverter(),appName);

            //use mysql to compensate.
            compensateProducer = new DefaultCompensateProducer(compensateManager);

            //use redis to compensate.
            redisCompensateProducer = new RedisCompensateProducer(compensateManager,appName,getJedisPool());

            if(messageStoreAndRetriever != null && !StringUtils.isEmpty(zkConnect)) {
                //jdbc 补偿实现
                final MessageCompensater messageCompensater = new MessageCompensater(messageStoreAndRetriever, channelFactory, getZkConnect(), getAppName());
                //start message compenstate.
                messageCompensater.start();
            }

            //Redis
            final RedisMessageCompensater redisMessageCompensater = new RedisMessageCompensater(appName,getJedisPool(),channelFactory,getMessageConverter());
            redisMessageCompensater.start();

        }

        if(getCoreSize() <= 0){
            coreSize = DEFAULT_CORE_SIZE;
        }

        if(getMaxSize() <= 0){
            maxSize = DEFAULT_MAX_SIZE;
        }

        executor = new ThreadPoolExecutor(coreSize, maxSize, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(500),
                    new NamedThreadFactory("rabbit-consumer-%s"));

        //add shutdownhook to release resource.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                executor.shutdown();
            }
        });

    }


    public ChannelFactory getChannelFactory() {
        return channelFactory;
    }

    public void setChannelFactory(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public HandlerService getHandlerService() {
        return handlerService;
    }

    public void setHandlerService(HandlerService handlerService) {
        this.handlerService = handlerService;
    }

    public boolean isNeedCompensation() {
        return needCompensation;
    }

    public void setNeedCompensation(boolean needCompensation) {
        this.needCompensation = needCompensation;
    }

    public MessageStoreAndRetriever getMessageStoreAndRetriever() {
        return messageStoreAndRetriever;
    }

    public void setMessageStoreAndRetriever(MessageStoreAndRetriever messageStoreAndRetriever) {
        this.messageStoreAndRetriever = messageStoreAndRetriever;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getZkConnect() {
        return zkConnect;
    }

    public void setZkConnect(String zkConnect) {
        this.zkConnect = zkConnect;
    }

    public int getCoreSize() {
        return coreSize;
    }

    public void setCoreSize(int coreSize) {
        this.coreSize = coreSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public ShardedJedisPool getShardedJedisPool() {
        return shardedJedisPool;
    }

    public void setShardedJedisPool(ShardedJedisPool shardedJedisPool) {
        this.shardedJedisPool = shardedJedisPool;
    }
}

