package rabbit.exception;

/**
 * Created by Allen lei on 2015/12/4.
 */
public class RabbitChannelException extends RuntimeException {

    public RabbitChannelException(String message, Throwable cause){
        super(message,cause);
    }

    public RabbitChannelException(String message){
        super(message);
    }

    public RabbitChannelException(Throwable cause){
        super(cause);
    }

}
