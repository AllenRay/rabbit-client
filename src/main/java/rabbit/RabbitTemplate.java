package  rabbit;

import rabbit.consumer.DelayMessageConsumer;
import rabbit.consumer.RetryMessageConsumer;
import rabbit.consumer.SimpleMessageConsumer;
import  rabbit.exception.RabbitChannelException;
import  rabbit.exception.RabbitConnectionException;
import  rabbit.handler.HandlerService;
import  rabbit.lyra.internal.util.Assert;
import  rabbit.messageConverter.MessageConverter;
import  rabbit.messageConverter.hessian.HessianMessageConverter;
import rabbit.sender.SimpleMessageSender;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringValueResolver;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

/**
 * Created by allen lei on 2015/12/7.
 * Rabbit template,提供send 和 receive
 *
 *
 *
 */
public class RabbitTemplate{

    private Logger logger = LoggerFactory.getLogger("#rabbitLog#");

    private ChannelFactory channelFactory;

    private MessageConverter messageConverter = new HessianMessageConverter();

    private HandlerService handlerService;

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
    public void sendMessage( rabbit.Message message,MessageChannelCallback messageChannelCallback){
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
    public void sendMessage( rabbit.Message message){
        sendMessage(message,getExchange(),getRoutingKey(),null);
    }

    /**
     *
     * @param message
     * @param exchange
     * @param routingKey
     */
    public void sendMessage( rabbit.Message message,String exchange,String routingKey){
        sendMessage(message,exchange,routingKey,null);
    }



    /**
     * 适用于fanout的exchange
     * @param message
     * @param exchange
     */
    public void sendMessage( rabbit.Message message,String exchange){
        sendMessage(message,exchange,"");
    }





    /**
     *
     * @param message
     * @param exchange
     * @param routingKey 特别注意routing key 如果你使用fanout，那么routing key 可以不传。但是记住别传null
     * @param basicProperties

     */
    public void sendMessage(final  rabbit.Message message, final String exchange, final String routingKey,AMQP.BasicProperties basicProperties){
        Assert.notNull(message);
        Assert.notNull(exchange);

        Channel channel = channelFactory.getChannel();
        try{
            SimpleMessageSender simepleMessageSender = new SimpleMessageSender(getMessageConverter(),channel,handlerService);
            simepleMessageSender.sendMessage(exchange,routingKey,message,basicProperties);
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
    public void sendMultiMessage(List< rabbit.Message> messages,String exchange,String routingKey,AMQP.BasicProperties basicProperties){
        Assert.notNull(messages);
        Assert.notNull(exchange);

        Channel channel = channelFactory.getChannel();
        try{
            SimpleMessageSender simepleMessageSender = new SimpleMessageSender(getMessageConverter(),channel,handlerService);
            simepleMessageSender.batchSendMessage(exchange,routingKey,messages,basicProperties);
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
    public void  sendMultiMessage(List< rabbit.Message> messages,String exchange,String routingKey){
        sendMultiMessage(messages,exchange,routingKey,null);
    }

    /**
     *fanout exchange
     * @param messages
     * @param exchange
     */
    public void  sendMultiMessage(List< rabbit.Message> messages,String exchange){
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
            consuming(queue, fetchCount, channel);
        }catch (Exception e){
            //will close channel if occur exception,like NullPointException and others.
            //so we need recovery channel after exception occured.
            logger.error("Receive message failed,the error is {} ",e);
            throw new RabbitChannelException("Receive message failed,the error is: "+e);

        }
    }

    /**
     * use rabbit delay message plugin to support delay.
     * the delay logic should be configuration.
     * please reference DelayQueueConfig
     * @param queue
     * @param fetchCount
     */
    public void receiveMessageWithDelay(final String queue,int fetchCount){
        final Channel channel = channelFactory.getChannel();
        try {
            //consuming(queue, fetchCount, channel);
            channel.basicQos(fetchCount);
            channel.basicConsume(queue,false,new DelayMessageConsumer(channel,this.handlerService,messageConverter,queue,this));
        }catch (Exception e){
            //will close channel if occur exception,like NullPointException and others.
            //so we need recovery channel after exception occured.
            logger.error("Receive message failed,the error is {} ",e);
            throw new RabbitChannelException("Receive message failed,the error is: "+e);

        }
    }

    public void receiveMessageWithDelay(final String queue){
        this.receiveMessageWithDelay(queue,1);
    }

    /**
     * use spring retry to support.
     * currently,the retry logic is hard code,
     * will retry three times.
     * @param queue
     * @param fetchCount
     */
    public void receiveMessageWithRetry(final String queue,int fetchCount){
        final Channel channel = channelFactory.getChannel();
        try {
            //consuming(queue, fetchCount, channel);
            channel.basicQos(fetchCount);
            channel.basicConsume(queue,false,new RetryMessageConsumer(channel,this.handlerService,messageConverter,queue));
        }catch (Exception e){
            //will close channel if occur exception,like NullPointException and others.
            //so we need recovery channel after exception occured.
            logger.error("Receive message failed,the error is {} ",e);
            throw new RabbitChannelException("Receive message failed,the error is: "+e);

        }
    }

    public void receiveMessageWithRetry(final String queue){
        this.receiveMessageWithRetry(queue,1);
    }


    /**
     * consuming message.
     * @param queue
     * @param fetchCount
     * @param channel
     * @throws IOException
     */
    private void consuming(final String queue, int fetchCount, final Channel channel) throws IOException {
        channel.basicQos(fetchCount);
        channel.basicConsume(queue,false,new SimpleMessageConsumer(channel,handlerService,getMessageConverter(),queue));
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


    @PostConstruct
    public void postInitAndCheck(){
        if(channelFactory == null){
            throw new RabbitConnectionException("Has no  rabbit connection channelFactory.");
        }

        //init handler
        //handlerService.initHandlerMap(this.context,this.stringValueResolver);

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
}

