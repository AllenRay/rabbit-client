package cn.rabbit.handler;


import cn.rabbit.Message;

/**
 *  Created by Allen lei on 2015/12/10.
 *  consumer handler
 *
 *  在接送到消息后根据queue找到对应的handler然后处理业务逻辑
 *  传入的对象是Message，真正的业务对象Message.messageBody.
 *  处理完业务逻辑，返回true则表明将会进行ACK，返回false表示业务处理不成功将不会进行ack。
 */
public abstract class ConsumerHandler implements Handler{



    public boolean handleMessage(Message message) {
        return handle(message);
    }

    public abstract boolean handle(Message message);

}
