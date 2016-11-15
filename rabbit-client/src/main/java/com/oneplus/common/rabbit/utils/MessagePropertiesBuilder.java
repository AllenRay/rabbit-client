package com.oneplus.common.rabbit.utils;

import com.rabbitmq.client.AMQP;

import java.util.Map;

/**
 * Created by allen lei on 2016/3/3.
 */
public class MessagePropertiesBuilder {

    public static AMQP.BasicProperties buildPropertiesWithHeader(Map<String,Object> header){
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().headers(header).build();
        return basicProperties;
    }

}
