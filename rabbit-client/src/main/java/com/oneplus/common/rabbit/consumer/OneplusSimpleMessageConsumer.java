package com.oneplus.common.rabbit.consumer;

import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.handler.Handler;
import com.oneplus.common.rabbit.handler.HandlerService;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.Channel;

/**
 * Created by allen lei on 2016/2/24.
 * simple consumer,
 * will use spring retry to support retry.
 */
public class OneplusSimpleMessageConsumer extends OnePlusDefaultMessageConsumer {


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public OneplusSimpleMessageConsumer(Channel channel, HandlerService handlerService, MessageConverter messageConverter, String queue) {
        super(channel, handlerService, messageConverter, queue);
    }

    @Override
    public boolean processMessage(Message message, Handler handler) {
        return handler.handleMessage(message);
    }


}