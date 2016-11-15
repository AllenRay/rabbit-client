package com.rabbit.producer;

import com.rabbit.Message;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * Created by allen lei on 2016/8/9.
 * version 1.0.0
 * company oneplus
 */
public interface CompensateProducer {

    /**
     * 发送有可能需要补偿的消息
     *
     * @param message
     * @param exchange
     * @param routingKey
     * @param messageKey 消息唯一的key，将会根据此key来删除消息，当消息ACK后。如果不传，将会使用messageID。
     * @param basicProperties
     * @param channel
     */
    void sendCompensationMessage(Message message, String exchange, String routingKey, String messageKey,
                                 AMQP.BasicProperties basicProperties, Channel channel);
}
