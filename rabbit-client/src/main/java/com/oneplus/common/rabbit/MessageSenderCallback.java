package com.oneplus.common.rabbit;

import com.rabbitmq.client.Channel;

/**
 * Created by allen lei on 2016/5/25.
 * version 1.0.0
 * company oneplus
 */
public interface MessageSenderCallback {

    void execute(Channel channel);
}
