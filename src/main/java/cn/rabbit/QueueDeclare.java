package cn.rabbit;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Map;

/**
 * Created by allen lei on 2015/12/8.
 * 声明queue
 */
public class QueueDeclare {

    private final Logger logger = LoggerFactory.getLogger(QueueDeclare.class);


    private String name;
    private boolean durable = true;
    private boolean exclusive = false;
    private boolean autoDelete = false;
    private Map<String,Object> arguments;

    public String getName() {
        return name;
    }

    private ChannelFactory channelFactory;

    @Required
    public void setName(String name) {
        this.name = name;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    public ChannelFactory getChannelFactory() {
        return channelFactory;
    }

    public void setChannelFactory(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @PostConstruct
    public void queueDeclare(){
        if(this.channelFactory == null){
            logger.error("Has no channelFactory,so cant declare queue");
            return;
        }

        Channel channel = channelFactory.getChannel();

        try {
            if(logger.isDebugEnabled()){
                logger.debug("start queue declare....");
            }
            channel.queueDeclare(getName(),isDurable(),isExclusive(),isAutoDelete(),getArguments());
        } catch (IOException e) {
            logger.error("Declare a queue occur error,the queue is {} and the error is {}",getName(),e);
        }finally {
            channelFactory.closeChannel(channel);
        }
    }


}
