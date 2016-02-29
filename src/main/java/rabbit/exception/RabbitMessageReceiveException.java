package rabbit.exception;

/**
 * Created by allen lei on 2015/12/8.
 */
public class RabbitMessageReceiveException extends RuntimeException {
    public RabbitMessageReceiveException(String message, Throwable cause){
        super(message,cause);
    }

    public RabbitMessageReceiveException(String message){
        super(message);
    }

    public RabbitMessageReceiveException(Throwable cause){
        super(cause);
    }
}
