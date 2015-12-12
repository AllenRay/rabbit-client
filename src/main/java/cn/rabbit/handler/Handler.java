package cn.rabbit.handler;


import cn.rabbit.Message;

/**
 * Created by Allen lei on 2015/12/10.
 *
 * 处理消息的接口，无论是发送还是接送
 */
public interface Handler {

    boolean handleMessage(Message message);

}
