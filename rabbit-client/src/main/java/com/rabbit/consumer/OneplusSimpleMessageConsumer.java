package com.rabbit.consumer;

import com.rabbit.Message;
import com.rabbit.handler.Handler;
import com.rabbit.handler.HandlerService;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.Channel;

import java.util.concurrent.ThreadPoolExecutor;

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
    public OneplusSimpleMessageConsumer(Channel channel, HandlerService handlerService, MessageConverter messageConverter, String queue,
                                        ThreadPoolExecutor executor) {
        super(channel, handlerService, messageConverter, queue,executor);
    }

    @Override
    public boolean processMessage(Message message, Handler handler) {
        return handler.handleMessage(message);
    }


}