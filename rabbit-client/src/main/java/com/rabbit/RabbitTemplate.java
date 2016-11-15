package com.rabbit;

import com.rabbit.consumer.OneplusRetryMessageConsumer;
import com.rabbit.consumer.OneplusSimpleMessageConsumer;
import com.rabbit.exception.RabbitChannelException;
import com.rabbit.handler.HandlerService;
import com.rabbit.lyra.internal.util.Assert;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbit.producer.*;
import com.rabbit.store.MessageCompensater;
import com.rabbit.store.MessageStoreAndRetriever;
import com.rabbit.store.MessageStoreBean;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Random;

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

    private Logger logger = LoggerFactory.getLogger(RabbitTemplate.class);

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

    private MessageStoreAndRetriever messageStoreAndRetriever;

    private boolean needCompensation = false;

    private String appName;

    private String zkConnect;

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
            channelFactory.returnProducerChannel(channel);
        }

    }


    /**
     * 使用一个channel 发送多个message。
     * 虽然是批量发送，但是每次发送完一个消息后，handler 都会处理下，但是无法将当前发送的信息传递给handler，
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

    public void sendCompensationMessage(Message message, String exchange) {
        this.sendCompensationMessage(message, exchange, "", null, null);
    }

    public void sendCompensationMessage(Message message, String exchange, String routingKey) {
        this.sendCompensationMessage(message, exchange, routingKey, null, null);
    }

    public void sendCompensationMessage(Message message, String exchange, String routingKey, AMQP.BasicProperties basicProperties) {
        this.sendCompensationMessage(message, exchange, routingKey, null, basicProperties);
    }

    /**
     * 发送需要补偿的消息
     *
     * @param message
     * @param exchange
     * @param routingKey
     * @param messageKey
     * @param basicProperties
     */
    public void sendCompensationMessage(Message message, String exchange, String routingKey, String messageKey, AMQP.BasicProperties basicProperties) {
        if (!isNeedCompensation()) {
            logger.error("The needCompensation must be set to true.");
            return;
        }
        Channel channel = channelFactory.getProducerChannel();
        try {
            compensateProducer.sendCompensationMessage(message, exchange, routingKey, messageKey, basicProperties, channel);
        } finally {
            //如果不清除，会把之前ACK的所有message都带上.
            channel.clearConfirmListeners();
            channelFactory.returnProducerChannel(channel);
        }
    }

    /**
     * store message.
     * use messageID to instead messageKey if messageKey is empty.
     *
     * @param message
     * @param exchange
     * @param routingKey
     * @param messageKey
     * @param basicProperties
     * @return
     */
    public void storeMessage(Message message, String exchange, String routingKey, String messageKey, AMQP.BasicProperties basicProperties) {
        MessageStoreBean messageStoreBean = new MessageStoreBean();
        messageStoreBean.setExchange(exchange);
        messageStoreBean.setRoutingKey(routingKey);
        messageStoreBean.setMessageKey(StringUtils.isEmpty(messageKey) ? message.getMessageId() : messageKey);
        messageStoreBean.setBasicProperties(basicProperties);
        messageStoreBean.setPayload(this.messageConverter.convertToMessage(message));
        this.messageStoreAndRetriever.storeMessage(messageStoreBean);
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
        channel.basicConsume(queue, false, getConsumerTag(),new OneplusSimpleMessageConsumer(channel, handlerService, getMessageConverter(), queue));
    }

    /**
     * get consumer tag.
     * @return
     */
    private String getConsumerTag(){
        String consumerTag;
        Random random = new Random(1000l);
        if(!StringUtils.isEmpty(getAppName())){
            consumerTag = "OnePlus:"+getAppName()+":"+Math.abs(random.nextLong());
        }else {
            consumerTag = "OnePlus:"+Math.abs(random.nextLong());
        }
        return consumerTag;
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
            channel.basicConsume(queue, autoACK, getConsumerTag(),new OneplusRetryMessageConsumer(channel, this.handlerService, messageConverter, queue));
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
            Assert.notNull(messageStoreAndRetriever);
            Assert.notNull(zkConnect);

            compensateProducer = new DefaultCompensateProducer(handlerService, messageStoreAndRetriever, messageConverter);

            final MessageCompensater messageCompensater = new MessageCompensater(messageStoreAndRetriever, channelFactory,getZkConnect());

            //start message compenstate.
            messageCompensater.start();

            //add shutdownhook to release resource.
            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run(){
                    messageCompensater.stop();
                }
            });
        }


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
}

