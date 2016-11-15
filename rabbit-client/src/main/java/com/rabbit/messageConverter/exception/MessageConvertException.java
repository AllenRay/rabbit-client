package com.rabbit.messageConverter.exception;

/**
 * Created by allen lei on 2016/11/15.
 * version 1.0.0
 * company oneplus
 */
public class MessageConvertException extends RuntimeException {

    public MessageConvertException(String message,Throwable e){
        super(message,e);
    }

    public MessageConvertException(Throwable e){
        super(e);
    }

}
