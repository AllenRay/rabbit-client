package com.oneplus.common.rabbit.handler;

import com.oneplus.common.rabbit.Message;

/**
 * Created by Allen lei on 2015/12/10.
 * producer handler.
 * 处理消息发送成功后业务逻辑
 *
 */
public abstract class ProducerHandler implements Handler {

    public boolean handleMessage(Message message){
        handle(message);
        return true;
    }

    public abstract void handle(Message message);


}
