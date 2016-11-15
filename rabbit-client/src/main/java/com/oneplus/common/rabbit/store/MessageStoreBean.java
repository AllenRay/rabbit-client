package com.oneplus.common.rabbit.store;

import com.rabbitmq.client.AMQP;

/**
 * Created by allen lei on 2016/8/8.
 * version 1.0.0
 * company oneplus
 */
public class MessageStoreBean {

    private String exchange;
    private String routingKey;
    private String messageKey; //唯一区别消息的可以标识。可以由业务方指定，也可以直接使用message的Id
    private AMQP.BasicProperties basicProperties;
    private byte [] payload;

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

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public AMQP.BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(AMQP.BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
