package com.oneplus.common.rabbit.producer;

import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.handler.Handler;
import com.oneplus.common.rabbit.handler.HandlerService;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

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


    protected boolean sendMessage(Channel channel, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] payload) throws IOException {
        channel.confirmSelect();
        channel.basicPublish(exchange, routingKey, properties, payload);
        try {
            boolean confirmaed = channel.waitForConfirms(5000l);
            //clear confirm listener
            if(confirmaed) {
                channel.clearConfirmListeners();
            }

            return confirmaed;
        } catch (InterruptedException | TimeoutException e) {
            logger.error("Wait message confirm occur error:" + e);
            return false;
        }
    }

    /**
     * @param handlerService
     */
    protected void preSendMessage(HandlerService handlerService, Channel channel, String exchange, String routingKey, final Message payload) {
        final Handler handler = handlerService.getProducerHandler(exchange, routingKey);

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                payload.setDeliveryTag(deliveryTag);
                payload.setAck(true);
                if (handler != null) {
                    handler.handleMessage(payload);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                payload.setDeliveryTag(deliveryTag);
                payload.setAck(false);

                if (handler != null) {
                    handler.handleMessage(payload);
                }
            }
        });
    }
}
