package com.oneplus.common.rabbit;

import com.oneplus.common.rabbit.utils.Constants;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Allen lei on 2015/12/8.
 * 声明exchange
 */
public class ExchangeDeclare {

    private Logger logger = LoggerFactory.getLogger(ExchangeDeclare.class);

    private String name;
    private String type;
    private boolean durable = true;
    private boolean autoDelete = false;
    private boolean internal = false;

    private String env;

    private Map<String,Object> arguments;

    private List<QueueBind> queueBinds;

    private ChannelFactory channelFactory;

    public String getName() {
        return name;
    }
    @Required
    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    @Required
    public void setType(String type) {
        this.type = type;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    public List<QueueBind> getQueueBinds() {
        return queueBinds;
    }

    public void setQueueBinds(List<QueueBind> queueBinds) {
        this.queueBinds = queueBinds;
    }

    public ChannelFactory getChannelFactory() {
        return channelFactory;
    }

    public void setChannelFactory(ChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    @PostConstruct
    public void exchangeDeclare(){
         if(this.channelFactory == null){
             logger.error("Has no channel factory,so cant declare exchange");
             return;
         }

         //为空或者线上环境，不能自动创建queue
         if(StringUtils.isEmpty(getEnv()) || (Constants.PRODUCTION.equals(getEnv()))){
             return;
         }

         Channel channel = this.channelFactory.createProducerChannel();

        try {
            channel.exchangeDeclare(getName(),getType(),isDurable(),isAutoDelete(),isInternal(),getArguments());
            if(queueBinds != null && !queueBinds.isEmpty()){
                for(QueueBind queueBind : queueBinds){
                   channel.queueBind(queueBind.getQueue(),getName(),queueBind.getRoutingKey(),queueBind.getArguments());
                }
            }
        } catch (IOException e) {
            logger.error("Declare exchange occur error,the exchange is {} and the erros is: {}",getName(),e);
        }finally {
            this.channelFactory.closeChannel(channel);
        }
    }

}
