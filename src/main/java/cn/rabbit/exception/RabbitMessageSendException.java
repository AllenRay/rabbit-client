package cn.rabbit.exception;

/**
 * Created by allen lei on 2015/12/8.
 */
public class RabbitMessageSendException extends RuntimeException {
    public RabbitMessageSendException(String message,Throwable cause){
        super(message,cause);
    }

    public RabbitMessageSendException(String message){
        super(message);
    }

    public RabbitMessageSendException(Throwable cause){
        super(cause);
    }
}
