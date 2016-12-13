package com.rabbit.producer.compensation;

import com.rabbit.Message;
import com.rabbitmq.client.AMQP;

/**
 * Created by leizhengyu on 11/28/16.
 */
public class CompensationMessage  {

    private String exchangeKey;
    private String routingKey;
    private Message message;
    private String messageKey;

    private int messageId;

    private AMQP.BasicProperties basicProperties;

    public String getExchangeKey() {
        return exchangeKey;
    }

    public void setExchangeKey(String exchangeKey) {
        this.exchangeKey = exchangeKey;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public AMQP.BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(AMQP.BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }
}
