package rabbit.messageConverter.hessian;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import rabbit.exception.RabbitMessageReceiveException;
import rabbit.exception.RabbitMessageSendException;
import rabbit.messageConverter.MessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Allen lei on 2015/12/9.
 * allen lei
 * hessian message converter.
 */
public class HessianMessageConverter implements MessageConverter, Serializable {

    private final Logger logger = LoggerFactory.getLogger(rabbit.messageConverter.hessian.HessianMessageConverter.class);

    public byte[] convertToMessage(Object t) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output(outputStream);
        try {
            hessian2Output.startMessage();
            hessian2Output.writeObject(t);
            hessian2Output.completeMessage();
            hessian2Output.close();
        } catch (IOException e) {
            logger.error("Hessian Serialize object occur error, the error is {}", e);
            throw new RabbitMessageSendException("Hessian serialize object occur error,the error is :" + e);
        }
        return outputStream.toByteArray();
    }

    public Object convertToObject(byte[] messages) {
        ByteArrayInputStream is = new ByteArrayInputStream(messages);
        Hessian2Input hessian2Input = new Hessian2Input(is);

        try {
            hessian2Input.startMessage();
            Object object = hessian2Input.readObject();
            hessian2Input.completeMessage();
            hessian2Input.close();
            is.close();
            return object;
        } catch (IOException e) {
            logger.error("Hessian deserialize object occur error,the error is: {}", e);
            throw new RabbitMessageReceiveException("Hessian deserialize object occur error,the error is: " + e);
        }
    }
}
