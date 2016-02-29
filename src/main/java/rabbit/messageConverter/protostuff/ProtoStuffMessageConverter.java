package rabbit.messageConverter.protostuff;

import rabbit.Message;
import rabbit.messageConverter.MessageConverter;
import io.protostuff.*;
import io.protostuff.runtime.ExplicitIdStrategy;
import io.protostuff.runtime.RuntimeSchema;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by allen lei on 2016/1/20.
 * use protostuff to ser/desr object.
 * 将会用于federation 为节省网络带宽
 */
public class ProtoStuffMessageConverter implements MessageConverter {

    private Schema schema;

    private ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry(20, 20, 20, 20, 20);

    private List<Class> registryPOJOClazz;


    @Override
    public byte[] convertToMessage(Object t) {
        LinkedBuffer buffer = LinkedBuffer.allocate(2048);
        try {
            byte[] bytes = ProtostuffIOUtil.toByteArray(t, schema, buffer);
            return bytes;
        } finally {
            buffer.clear();
        }
    }

    @Override
    public Object convertToObject(byte[] messages) {
        Object o = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(messages, o, schema);
        return o;
    }

    @PostConstruct
    public void initSchema() {

        registry.registerCollection(CollectionSchema.MessageFactories.ArrayList, 2);
        registry.registerCollection(CollectionSchema.MessageFactories.HashSet, 3);
        registry.registerCollection(CollectionSchema.MessageFactories.LinkedList, 4);
        registry.registerCollection(CollectionSchema.MessageFactories.CopyOnWriteArrayList, 6);
        registry.registerCollection(CollectionSchema.MessageFactories.CopyOnWriteArraySet, 7);

        registry.registerMap(MapSchema.MessageFactories.HashMap, 2);
        registry.registerMap(MapSchema.MessageFactories.LinkedHashMap, 3);
        registry.registerMap(MapSchema.MessageFactories.ConcurrentHashMap, 4);
        registry.registerMap(MapSchema.MessageFactories.TreeMap, 6);


        if (getRegistryPOJOClazz() == null || getRegistryPOJOClazz().isEmpty()) {
            return;
        }
        int i = 0;
        for (Class clss : getRegistryPOJOClazz()) {
            i++;
            registry.registerPojo(clss, i + 1);
        }

        schema = RuntimeSchema.getSchema(Message.class, registry.strategy);
    }

    public List<Class> getRegistryPOJOClazz() {
        return registryPOJOClazz;
    }

    public void setRegistryPOJOClazz(List<Class> registryPOJOClazz) {
        this.registryPOJOClazz = registryPOJOClazz;
    }
}
