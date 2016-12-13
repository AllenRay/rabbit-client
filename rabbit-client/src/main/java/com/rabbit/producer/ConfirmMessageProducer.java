package com.rabbit.producer;

import com.rabbit.Message;
import com.rabbit.handler.Handler;
import com.rabbit.handler.HandlerService;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by allen lei on 2016/8/2.
 * version 1.0.0
 * company oneplus
 */
public class ConfirmMessageProducer extends DefaultMessageProducer{

    private final Logger logger = LoggerFactory.getLogger(ConfirmMessageProducer.class);

    /**
     * build retry template
     *
     * @param messageConverter
     * @param handlerService
     */
    public ConfirmMessageProducer(MessageConverter messageConverter, HandlerService handlerService) {
        super(messageConverter, handlerService);
    }


    protected boolean sendMessage(Channel channel, String exchange, String routingKey, AMQP.BasicProperties properties, List<Message> messages) throws IOException {
        channel.confirmSelect();
        for(Message message : messages) {
            byte payload[] = getMessageConverter().convertToMessage(message);
            logger.info("{} message {}",message.getRequestId(),message.getMessageId());
            channel.basicPublish(exchange, routingKey, properties, payload);
        }
        try {
           return channel.waitForConfirms();
        } catch (InterruptedException e) {
            logger.error("Wait message confirm occur error:" + e);
            return false;
        }
    }

    /**
     * @param handlerService
     */
    protected void preSendMessage(HandlerService handlerService, Channel channel, String exchange, String routingKey, final List<Message> payloads) {
        final Handler handler = handlerService.getProducerHandler(exchange, routingKey);

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                if(handler == null){
                    return;
                }
                for(Message payload : payloads) {
                    payload.setDeliveryTag(deliveryTag);
                    payload.setAck(true);
                    handler.handleMessage(payload);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                if(handler == null){
                    return;
                }
                for(Message payload : payloads) {
                    payload.setDeliveryTag(deliveryTag);
                    payload.setAck(false);
                    handler.handleMessage(payload);
                }
            }
        });
    }
}
