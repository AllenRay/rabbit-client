package com.oneplus.common.rabbit;

import com.rabbitmq.client.Channel;

/**
 * Created by Allen lei on 2015/12/7.
 */
public interface MessageChannelCallback {

    void handleChannel(byte[] bytes, Channel channel);
}
