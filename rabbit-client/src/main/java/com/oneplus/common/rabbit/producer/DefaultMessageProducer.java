package com.oneplus.common.rabbit.producer;

import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.exception.RabbitMessageSendException;
import com.oneplus.common.rabbit.handler.Handler;
import com.oneplus.common.rabbit.handler.HandlerService;
import com.oneplus.common.rabbit.lyra.internal.util.Assert;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import com.oneplus.common.rabbit.store.MessageStore;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by allen lei on 2016/2/26.
 * default message producer.
 * if send failed,will retry.
 */
public class DefaultMessageProducer implements Producer {

    private final RetryTemplate retryTemplate = new RetryTemplate();

    private final Logger logger = LoggerFactory.getLogger(DefaultMessageProducer.class);

    private MessageConverter messageConverter;

    private HandlerService handlerService;

    private MessageStore messageStore;



    //use min 5  and max 20 threads pool to send asynchronous message.
    //but the maxiumn message count is 4000,if not necessary please dont send async message.
    private final ExecutorService executorService = new ThreadPoolExecutor(5,20,60, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(200),new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * build retry template
     */
    public DefaultMessageProducer(MessageConverter messageConverter, HandlerService handlerService){

        Assert.notNull(messageConverter);
        Assert.notNull(handlerService);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setMultiplier(4.0d);
        backOffPolicy.setInitialInterval(3000L);
        backOffPolicy.setMaxInterval(60000L);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        //only retry runtime exception.
        Map<Class<? extends Throwable>,Boolean> exceptionsRetryMap = new HashMap<>();
        exceptionsRetryMap.put(Exception.class,true);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3,exceptionsRetryMap);
        retryTemplate.setRetryPolicy(simpleRetryPolicy);

        this.messageConverter = messageConverter;
        this.handlerService = handlerService;
    }

    /**
     * send single message.
     * @param exchange
     * @param routingKey
     * @param payload
     * @param basicProperties
     */
    @Override
    public void sendMessage(String exchange, String routingKey, Message payload, AMQP.BasicProperties basicProperties, Channel channel){
        sendMessageWithRetry(exchange,routingKey,payload,basicProperties,channel);
    }

    /**
     * 异步发送消息
     *
     * @param exchange
     * @param routingKey
     * @param payload
     * @param basicProperties
     */
    @Override
    public void sendAsyncMessage(final String exchange, final String routingKey, final Message payload, final AMQP.BasicProperties basicProperties, final Channel channel){
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                sendMessage(exchange,routingKey,payload,basicProperties,channel);
            }
        });
    }

    /**
     * batch send async message.
     *
     * @param exchange
     * @param routingKey
     * @param payloads
     * @param basicProperties
     */
    @Override
    public void batchSendAsyncMessages(final String exchange, final String routingKey, final List<Message> payloads, final AMQP.BasicProperties basicProperties,final Channel channel){
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                batchSendMessage(exchange,routingKey,payloads,basicProperties,channel);
            }
        });
    }

    /**
     * batch send messages.
     * @param exchange
     * @param routingKey
     * @param payloads
     * @param basicProperties
     */
    @Override
    public void batchSendMessage(String exchange,String routingKey,List<Message> payloads,AMQP.BasicProperties basicProperties,Channel channel){
        Assert.notNull(payloads);
        for (Message payload : payloads){
            sendMessageWithRetry(exchange,routingKey,payload,basicProperties,channel);
        }
    }






    /**
     * send message with retry.
     * @param exchange
     * @param routingKey
     * @param payload
     * @param basicProperties
     */
    private void sendMessageWithRetry(final String exchange, final String routingKey, final Message payload, final AMQP.BasicProperties basicProperties,final Channel channel){
        Assert.notNull(exchange);
        Assert.notNull(payload);

        preSendMessage(this.handlerService,channel,exchange,routingKey,payload);

        final byte [] messageBody = this.messageConverter.convertToMessage(payload);

        retryTemplate.execute(new RetryCallback<Boolean, RabbitMessageSendException>() {

            @Override
            public Boolean doWithRetry(RetryContext context) throws RabbitMessageSendException {
                try {
                    logger.info("Send message {}",payload.getMessageId());

                    Long currentTime = System.currentTimeMillis();
                    boolean successful = sendMessage(channel,exchange,routingKey,basicProperties,messageBody);
                    if(!successful){
                        long endTime = System.currentTimeMillis();
                        long used = (endTime-currentTime);
                        logger.warn("#NACK exchange{} routingKey{} requestId {}# send message use {} ms", exchange, routingKey, payload.getRequestId(), used);
                        throw new RabbitMessageSendException("Send message failed,will retry.");
                    }
                    long endTime = System.currentTimeMillis();
                    long used = (endTime-currentTime);
                    logger.info("#ACK exchange {} routingKey {} requestId {}# send message use {} ms", exchange, routingKey, payload.getRequestId(), used);
                } catch (Exception e) {
                    throw new RabbitMessageSendException("Send message occur error,will retry.",e);
                }
                return true;
            }
        });
    }

    /**
     * default send no need confirm message.
     * @param channel
     * @param exchange
     * @param routingKey
     * @param properties
     * @param payload
     * @return
     * @throws IOException
     */
    protected boolean sendMessage(Channel channel, String exchange, String routingKey, AMQP.BasicProperties properties,byte[] payload) throws IOException {
        channel.basicPublish(exchange,routingKey,properties,payload);
        return true;
    }

    /**
     *
     * @param handlerService
     */
    protected void preSendMessage(HandlerService handlerService,Channel channel,String exchange,String routingKey,Message message){

    }
}
