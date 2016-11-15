package com.oneplus.common.rabbit.producer;

import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.handler.Handler;
import com.oneplus.common.rabbit.handler.HandlerService;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import com.oneplus.common.rabbit.store.MessageStore;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.locks.Condition;

/**
 * Created by allen lei on 2016/8/9.
 * version 1.0.0
 * company oneplus
 */
public class DefaultCompensateProducer implements CompensateProducer {

    private final static Logger logger = LoggerFactory.getLogger(DefaultCompensateProducer.class);

    private HandlerService handlerService;

    private MessageStore messageStore;

    private MessageConverter messageConverter;

    public DefaultCompensateProducer(HandlerService handlerService,MessageStore messageStore,MessageConverter messageConverter){
        this.handlerService = handlerService;
        this.messageConverter = messageConverter;
        this.messageStore = messageStore;
    }

    @Override
    public void sendCompensationMessage(final Message message, String exchange, String routingKey, final String messageKey,
                                         AMQP.BasicProperties basicProperties, Channel channel) {

        final Handler handler = handlerService.getProducerHandler(exchange,routingKey);

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                  //after ACK,remove message from DB.
                  if(StringUtils.isEmpty(messageKey)){
                      logger.info("Remove Message: {} after Broker ACK.",message.getMessageId());
                      messageStore.removeMessage(message.getMessageId());
                  }else {
                      logger.info("Remove Message: {} after Broker ACK.",messageKey);
                      messageStore.removeMessage(messageKey);
                  }
                  message.setAck(true);
                  message.setDeliveryTag(deliveryTag);
                  if(handler != null) {
                      handler.handleMessage(message);

                  }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                message.setAck(false);
                message.setDeliveryTag(deliveryTag);

                if(handler != null) {
                    handler.handleMessage(message);
                }
            }
        });

        byte [] payloads = this.messageConverter.convertToMessage(message);

        try{
            long start = System.currentTimeMillis();
            channel.confirmSelect();
            channel.basicPublish(exchange,routingKey,basicProperties,payloads);
            //wait all confirm listeners executed completed.
            channel.waitForConfirms(5000l);
            long end = System.currentTimeMillis();
            long spent = end - start;
            logger.debug("Send compenstation message spent {} ms",spent);
        }catch (Throwable e){
            logger.error("Confirm message failed, will compenstate message."+e);
        }
    }
}
