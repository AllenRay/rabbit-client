package com.oneplus.common.rabbit.producer;

import com.oneplus.common.rabbit.Message;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.util.List;

/**
 * Created by allen lei on 2016/5/25.
 * version 1.0.0
 * company oneplus
 */
public interface Producer {

    void sendMessage(final String exchange, final String routingKey, final Message payload, final AMQP.BasicProperties basicProperties, Channel channel);

    void sendAsyncMessage(final String exchange, final String routingKey, final Message payload, final AMQP.BasicProperties basicProperties, Channel channel);

    void batchSendAsyncMessages(final String exchange, final String routingKey, final List<Message> payloads, final AMQP.BasicProperties basicProperties, Channel channel);

    void batchSendMessage(final String exchange, final String routingKey, final List<Message> payloads, final AMQP.BasicProperties basicProperties, Channel channel);

}
