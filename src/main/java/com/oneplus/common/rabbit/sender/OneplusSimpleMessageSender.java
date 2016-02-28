package com.oneplus.common.rabbit.sender;

import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.exception.RabbitMessageSendException;
import com.oneplus.common.rabbit.handler.Handler;
import com.oneplus.common.rabbit.handler.HandlerService;
import com.oneplus.common.rabbit.lyra.internal.util.Assert;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import com.oneplus.common.rabbit.retry.OneplusRetryTemplate;
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
import java.util.concurrent.TimeoutException;

/**
 * Created by allen lei on 2016/2/26.
 * message sender
 * if send failed,will retry.
 */
public class OneplusSimpleMessageSender {

    private final RetryTemplate retryTemplate = new OneplusRetryTemplate();

    private final Logger logger = LoggerFactory.getLogger("#Message_Sender#");

    private MessageConverter messageConverter;

    private HandlerService handlerService;

    private Channel channel;

    /**
     * build retry template
     */
    public OneplusSimpleMessageSender(MessageConverter messageConverter, Channel channel, HandlerService handlerService){

        Assert.notNull(messageConverter);
        Assert.notNull(channel);

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
        this.channel = channel;
        this.handlerService = handlerService;
    }

    /**
     * send single message.
     * @param exchange
     * @param routingKey
     * @param payload
     * @param basicProperties
     */
    public void sendMessage(String exchange,String routingKey,Message payload,AMQP.BasicProperties basicProperties){
        sendMessageWithRetry(exchange,routingKey,payload,basicProperties);
    }

    /**
     * batch send messages.
     * @param exchange
     * @param routingKey
     * @param payloads
     * @param basicProperties
     */
    public void batchSendMessage(String exchange,String routingKey,List<Message> payloads,AMQP.BasicProperties basicProperties){
        Assert.notNull(payloads);
        for (Message payload : payloads){
            sendMessageWithRetry(exchange,routingKey,payload,basicProperties);
        }
    }






    /**
     * send message with retry.
     * @param exchange
     * @param routingKey
     * @param payload
     * @param basicProperties
     */
    private void sendMessageWithRetry(final String exchange, final String routingKey, final Message payload, final AMQP.BasicProperties basicProperties){
        Assert.notNull(channel);
        Assert.notNull(exchange);
        Assert.notNull(payload);

        final Handler handler = this.handlerService.getProducerHandler(exchange,routingKey);
        final Long currentTime = System.currentTimeMillis();

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                payload.setDeliveryTag(deliveryTag);
                payload.setAck(true);

                if(handler != null) {
                    handler.handleMessage(payload);
                }
                long endTime = System.currentTimeMillis();
                long used = (endTime-currentTime);
                if(logger.isInfoEnabled()) {
                    logger.info("#ACK exchange {} routingKey {} requestId {}# send message use {} ms", exchange, routingKey, payload.getRequestId(), used);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                payload.setDeliveryTag(deliveryTag);
                payload.setAck(false);

                if(handler != null) {
                    handler.handleMessage(payload);
                }
                long endTime = System.currentTimeMillis();
                long used = (endTime-currentTime);
                if(logger.isWarnEnabled()) {
                    logger.warn("#NACK exchange{} routingKey{} requestId {}# send message use {} ms", exchange, routingKey, payload.getRequestId(), used);
                }
            }
        });

        final byte [] messageBody = this.messageConverter.convertToMessage(payload);

        retryTemplate.execute(new RetryCallback<Boolean, RabbitMessageSendException>() {

            @Override
            public Boolean doWithRetry(RetryContext context) throws RabbitMessageSendException {
                try {
                    logger.info("Send message {}",payload.getMessageId());

                    channel.confirmSelect();
                    channel.basicPublish(exchange,routingKey,basicProperties,messageBody);
                    boolean successful = channel.waitForConfirms(10000L);
                    if(!successful){
                        throw new RabbitMessageSendException("Send message failed,will retry.");
                    }

                    logger.info("Send message {} successful",payload.getMessageId());
                } catch (IOException | InterruptedException | TimeoutException e) {
                    if(logger.isErrorEnabled()){
                        logger.error("Send message occur error,the error is {}",e.getMessage());
                    }
                    throw new RabbitMessageSendException("Send message occur error,will retry.");
                }
                return true;
            }
        });
    }
}
