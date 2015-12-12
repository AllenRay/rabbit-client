package cn.rabbit;


import cn.rabbit.exception.RabbitChannelException;
import cn.rabbit.exception.RabbitConnectionException;
import cn.rabbit.exception.RabbitMessageSendException;
import cn.rabbit.handler.Handler;
import cn.rabbit.handler.HandlerService;
import cn.rabbit.messageConverter.MessageConverter;
import cn.rabbit.messageConverter.hessian.HessianMessageConverter;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.util.StringValueResolver;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

/**
 * Created by allen lei on 2015/12/7.
 * Rabbit template,提供send 和 receive
 *
 *
 *
 */
public class RabbitTemplate implements ApplicationContextAware, EmbeddedValueResolverAware{

    private Logger logger = LoggerFactory.getLogger(RabbitTemplate.class);

    private ChannelFactory channelFactory;

    private MessageConverter messageConverter = new HessianMessageConverter();

    private HandlerService handlerService = new HandlerService();

    private String queue;

    private String exchange;

    private String routingKey = "";

    private ApplicationContext context;

    private StringValueResolver stringValueResolver;


    /**
     * 发送消息的low level API，
     * 将只会处理消息的序列化和channel的获取和关闭。
     * 客户端程序在拿到channel实现自己的业务逻辑。
     * @param message
     * @param messageChannelCallback
     */
    public void sendMessage(Message message,MessageChannelCallback messageChannelCallback){
        Channel channel = channelFactory.getChannel();
        try {
            byte[] messages = getMessageConverter().convertToMessage(message);
            messageChannelCallback.handleChannel(messages, channel);
        }finally {
            channelFactory.closeChannel(channel);
        }
    }

    /**
     *
     * @param message
     */
    public void sendMessage(Message message){
        sendMessage(message,getExchange(),getRoutingKey(),null);
    }

    /**
     *
     * @param message
     * @param exchange
     * @param routingKey
     */
    public void sendMessage(Message message,String exchange,String routingKey){
        sendMessage(message,exchange,routingKey,null);
    }



    /**
     * 适用于fanout的exchange
     * @param message
     * @param exchange
     */
    public void sendMessage(Message message,String exchange){
        sendMessage(message,exchange,"");
    }





    /**
     *
     * @param message
     * @param exchange
     * @param routingKey 特别注意routing key 如果你使用fanout，那么routing key 可以不传。但是记住别传null
     * @param basicProperties

     */
    public void sendMessage(final Message message, final String exchange, final String routingKey,AMQP.BasicProperties basicProperties){
        Channel channel = channelFactory.getChannel();
        try{

            final Handler handler = this.handlerService.getProducerHandler(exchange,routingKey);
            final Long currentTime = System.currentTimeMillis();

            channel.addConfirmListener(new ConfirmListener() {
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    message.setDeliveryTag(deliveryTag);
                    message.setAck(true);

                    if(handler != null) {
                        handler.handleMessage(message);
                    }
                    long endTime = System.currentTimeMillis();
                    long used = (endTime-currentTime);
                    logger.error("#ACK exchange {} routingKey {} requestId {}# send message use {} ms",exchange,routingKey,message.getRequestId(),used);
                }

                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    message.setDeliveryTag(deliveryTag);
                    message.setAck(false);

                    if(handler != null) {
                        handler.handleMessage(message);
                    }
                    long endTime = System.currentTimeMillis();
                    long used = (endTime-currentTime);
                    logger.error("#NACK exchange{} routingKey{} requestId {}# send message use {} ms",exchange,routingKey,message.getRequestId(),used);
                }
            });

            byte [] bytes = getMessageConverter().convertToMessage(message);
            channel.confirmSelect();
            channel.basicPublish(exchange,routingKey,basicProperties,bytes);
            channel.waitForConfirmsOrDie();

        }catch (IOException e){
            logger.error("Send message occur error,the exchange is {} and the routingkey is {}",getExchange(),getRoutingKey());
            throw new RabbitMessageSendException("Send message occur error,please check,the error is :"+e);
        }catch (InterruptedException e){
            logger.error("Send message occur error,the exchange is {} and the routingkey is {}",getExchange(),getRoutingKey());
            throw new RabbitMessageSendException("Send message occur error,please check,the error is :"+e);
        }finally {
            channelFactory.closeChannel(channel);
        }
    }

    /**
     * 使用一个channel 发送多个message。
     * 虽然是批量发送，但是每次发送完一个消息后，handler 都会处理下，但是无法将当前发送的信息传递给handler，
     * handler得到的只是deliveryTag以及ACK是否确认
     * @param messages
     * @param exchange
     * @param routingKey
     * @param basicProperties
     */
    public void sendMultiMessage(List<Message> messages,String exchange,String routingKey,AMQP.BasicProperties basicProperties){
        Channel channel = channelFactory.getChannel();
        try{
            final Handler handler = handlerService.getProducerHandler(exchange,routingKey);

            final Long currentTime = System.currentTimeMillis();

            channel.addConfirmListener(new ConfirmListener() {
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    Message message = new Message();
                    message.setDeliveryTag(deliveryTag);
                    message.setAck(true);
                    if(handler != null){
                        handler.handleMessage(message);
                    }
                }

                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    Message message = new Message();
                    message.setDeliveryTag(deliveryTag);
                    message.setAck(false);
                    if(handler != null){
                        handler.handleMessage(message);
                    }
                }
            });
            for(Object message : messages) {
                byte[] bytes = getMessageConverter().convertToMessage(message);
                channel.confirmSelect();
                channel.basicPublish(exchange, routingKey, basicProperties, bytes);
                channel.waitForConfirmsOrDie();
            }
        }catch (IOException e){
            logger.error("Send message occur error,the exchange is {} and the routingkey is {}",getExchange(),getRoutingKey());
            throw new RabbitMessageSendException("Send message occur error,please check,the error is :"+e);
        }catch (InterruptedException e){
            logger.error("Send message occur error,the exchange is {} and the routingkey is {}",getExchange(),getRoutingKey());
            throw new RabbitMessageSendException("Send message occur error,please check,the error is :"+e);
        }finally {
            channelFactory.closeChannel(channel);
        }
    }

    /**
     *
     * @param messages
     * @param exchange
     * @param routingKey
     */
    public void  sendMultiMessage(List<Message> messages,String exchange,String routingKey){
        sendMultiMessage(messages,exchange,routingKey,null);
    }

    /**
     *fanout exchange
     * @param messages
     * @param exchange
     */
    public void  sendMultiMessage(List<Message> messages,String exchange){
        sendMultiMessage(messages,exchange,"",null);
    }

    /**
     *
     * @param messages
     */
    public void sendMultiMessage(List<Message> messages){
        sendMultiMessage(messages,getExchange(),getRoutingKey(),null);
    }




    /**
     *
     * @param queue
     * @param fetchCount 每次消费多少条消息
     */
    public void receive(final String queue, int fetchCount){
        final Channel channel = channelFactory.getChannel();
        try {
            channel.basicQos(fetchCount);
            channel.basicConsume(queue,false,new DefaultConsumer(channel){
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    Object message = getMessageConverter().convertToObject(body);
                    if(message instanceof  Message){
                        Handler handler = handlerService.getConsumerHandler(queue);
                        if(handler != null) {
                            Message msg = (Message) message;
                            msg.setDeliveryTag(envelope.getDeliveryTag());
                            msg.setCustomerTag(consumerTag);
                            boolean success = handler.handleMessage(msg);
                            if (success) {
                                channel.basicAck(envelope.getDeliveryTag(), false);
                            }else {
                                //no ack and re queue.
                                channel.basicNack(envelope.getDeliveryTag(),false,true);
                            }
                        }else {
                            //no handler will be re queue  and no ack.
                            channel.basicNack(envelope.getDeliveryTag(),false,true);
                            logger.error("The queue {} has no handler,so this message will not consumed.",queue);
                        }
                    }else {
                        //if the message is not Message,will be discard.
                        channel.basicNack(envelope.getDeliveryTag(),false,false);
                        logger.error("invalid message,the valid message type should be {} but current message type is {}",Message.class.getName(),
                                message != null ? message.getClass().getName() : "null");
                    }
                }
            });

        }catch (IOException e){
            logger.error("Receive message failed,the error is {} ",e);
            throw new RabbitChannelException("Receive message failed,the error is: "+e);
        }

    }

    /**
     * use QueueConsumer to consumer message
     * @param queue
     */
    public void receiveMessageUseQueue(final String queue){
        final Channel channel = channelFactory.getChannel();
        try{
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queue,false,consumer);
            while (true){
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                byte [] messageBody = delivery.getBody();
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                Object msg = getMessageConverter().convertToObject(messageBody);
                if(msg instanceof  Message){
                    Handler handler = handlerService.getConsumerHandler(queue);
                    if(handler != null) {
                        Message message = (Message) msg;
                        message.setDeliveryTag(deliveryTag);
                        boolean success = handler.handleMessage(message);
                        if (success) {
                            channel.basicAck(deliveryTag, false);
                        }else {
                            //no ack and re queue
                            channel.basicNack(deliveryTag,false,true);
                        }
                    }else {
                        channel.basicNack(deliveryTag,false,true);
                        logger.error("The queue {} has no handler,so this message will not consumed.",queue);
                    }
                }else {
                    channel.basicNack(deliveryTag,false,false);
                    logger.error("invalid message,the valid message type should be {} but current message type is {}",Message.class.getName(),
                            msg != null ? msg.getClass().getName() : "null");
                }
            }
        }catch (IOException e){
            logger.error("Receive message failed,the error is {} ",e);
            throw new RabbitChannelException("Receive message failed,the error is: "+e);
        }catch (InterruptedException e){
            logger.error("Receive message failed,the error is {} ",e);
            throw new RabbitChannelException("Receive message failed,the error is: "+e);
        }
    }

    /**
     *默认每次消费一条
     * @param queue
     */
    public void receive(String queue){
        receive(queue,1);
    }

    /**
     * 默认fetchcount是1，默认每次小费一条消息
     */
    public void receive(){
        receive(getQueue(),1);
    }

    /**
     * 传入channel 进行处理
     * @param channelCallback
     */
    public void execute(ChannelCallback channelCallback){
        Channel channel = channelFactory.getChannel();
        try {
            channelCallback.handleChannelCallback(channel);
        }finally {
            channelFactory.closeChannel(channel);
        }
    }


    @PostConstruct
    public void postInitAndCheck(){
        if(channelFactory == null){
            throw new RabbitConnectionException("Has no rabbit connection channelFactory.");
        }

        //init handler
        handlerService.initHandlerMap(this.context,this.stringValueResolver);

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

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
        this.stringValueResolver = stringValueResolver;
    }
}

