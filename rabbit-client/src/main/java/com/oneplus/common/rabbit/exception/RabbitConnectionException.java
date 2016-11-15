package com.oneplus.common.rabbit.exception;

/**
 * Created by Allen lei on 2015/12/4.
 */
public class RabbitConnectionException extends RuntimeException {

    public RabbitConnectionException(String message,Throwable cause){
        super(message,cause);
    }

    public RabbitConnectionException(String message){
        super(message);
    }

    public RabbitConnectionException(Throwable cause){
        super(cause);
    }

}
