package cn.rabbit.handler;

import cn.rabbit.Message;

/**
 * Created by Allen lei on 2015/12/10.
 * producer handler.
 * 在消息发送成功后将会根据Exchange 和 routing key 来寻找对应的handler，处理消息发送成功后业务逻辑
 *
 */
public abstract class ProducerHandler implements Handler {



    public boolean handleMessage(Message message){
        handle(message);
        return true;
    }

    public abstract void handle(Message message);



}
