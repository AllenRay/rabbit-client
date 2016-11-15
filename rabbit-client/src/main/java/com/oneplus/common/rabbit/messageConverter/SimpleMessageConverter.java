package com.oneplus.common.rabbit.messageConverter;


import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.oneplus.common.rabbit.messageConverter.exception.MessageConvertException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by allen lei on 2016/3/8.
 * use hessian
 */
public class SimpleMessageConverter implements MessageConverter {


    @Override
    public byte[] convertToMessage(Object t) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Hessian2Output hessian2Output = new Hessian2Output(outputStream);
        try {
            hessian2Output.startMessage();
            hessian2Output.writeObject(t);
            hessian2Output.completeMessage();
            hessian2Output.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new MessageConvertException("Hessian serialize object occur error",e);
        }
    }

    @Override
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
            throw new MessageConvertException("Hessian deserialize object occur error",e);
        }

    }

}
