package com.rabbit.messageConverter;

/**
 * Created by allen lei on 2015/12/7.
 * 序列化消息的接口
 */
public interface MessageConverter {

    byte[] convertToMessage(Object t);

    Object convertToObject(byte[] messages);

}
