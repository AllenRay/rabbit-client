package cn.rabbit.messageConverter.kyro;

import cn.rabbit.messageConverter.MessageConverter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Allen lei on 2015/12/7.
 * 使用kryo来转换消息
 */
public class KryoMessageConverter implements MessageConverter {

    private final KryoFactory kryoFactory = KryoFactory.getDefaultFactory();

    public byte[] convertToMessage(Object o) {
        Kryo kryo = kryoFactory.getKryo();
        Output output = new Output(1024,-1);
        kryo.writeClassAndObject(output,o);
        byte [] bytes = output.getBuffer();
        kryoFactory.returnKryo(kryo);
        return bytes;
    }

    public Object convertToObject(byte[] messages) {
        Kryo kryo = kryoFactory.getKryo();
        Input input = new Input(messages);
        Object o = kryo.readClassAndObject(input);
        kryoFactory.returnKryo(kryo);
        return o;
    }
}
