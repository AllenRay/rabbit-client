package com.oneplus.common.rabbit;

import com.oneplus.common.rabbit.utils.Constants;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
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

    //queue properties
    private Integer messageTTL = -1;
    private Integer expires = -1;
    private Integer maxLength = -1;
    private Integer messageMaxLengthByte = -1;
    private String deadLetterExchange;
    private String deadLetterRoutingKey;
    private Integer maxProperty = -1;


    public String getName() {
        return name;
    }

    private ChannelFactory channelFactory;

    private String env;

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

    public Integer getMessageTTL() {
        return messageTTL;
    }

    public void setMessageTTL(Integer messageTTL) {
        this.messageTTL = messageTTL;
    }

    public Integer getExpires() {
        return expires;
    }

    public void setExpires(Integer expires) {
        this.expires = expires;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public Integer getMessageMaxLengthByte() {
        return messageMaxLengthByte;
    }

    public void setMessageMaxLengthByte(Integer messageMaxLengthByte) {
        this.messageMaxLengthByte = messageMaxLengthByte;
    }

    public String getDeadLetterExchange() {
        return deadLetterExchange;
    }

    public void setDeadLetterExchange(String deadLetterExchange) {
        this.deadLetterExchange = deadLetterExchange;
    }

    public String getDeadLetterRoutingKey() {
        return deadLetterRoutingKey;
    }

    public void setDeadLetterRoutingKey(String deadLetterRoutingKey) {
        this.deadLetterRoutingKey = deadLetterRoutingKey;
    }

    public Integer getMaxProperty() {
        return maxProperty;
    }

    public void setMaxProperty(Integer maxProperty) {
        this.maxProperty = maxProperty;
    }

    @PostConstruct
    public void queueDeclare() {
        if (this.channelFactory == null) {
            logger.error("Has no channelFactory,so cant declare queue");
            return;
        }
        //empty or production will not automatic create queue
        if (StringUtils.isEmpty(getEnv()) || (Constants.PRODUCTION.equals(getEnv()))) {
            return;
        }
        Channel channel = channelFactory.createProducerChannel();
        //set queue args.
        Map<String, Object> args = new HashMap<>();
        if(getMessageTTL() > 0){
            args.put("x-message-ttl",getMessageTTL());
        }
        if(getExpires() > 0){
            args.put("x-expires",getExpires());
        }
        if(getMaxLength() > 0){
            args.put("x-max-length",getMaxLength());
        }
        if(getMessageMaxLengthByte() > 0){
            args.put("x-max-length-bytes",getMessageMaxLengthByte());
        }
        if(getMaxProperty() > 0){
            args.put("x-max-priority",getMaxProperty());
        }
        if(!StringUtils.isEmpty(getDeadLetterExchange())){
            args.put("x-dead-letter-exchange",getDeadLetterExchange());
        }
        if(!StringUtils.isEmpty(getDeadLetterRoutingKey())){
            args.put("x-dead-letter-routing-key",getDeadLetterRoutingKey());
        }

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("start queue declare....");
            }
            channel.queueDeclare(getName(), isDurable(), isExclusive(), isAutoDelete(), args);
        } catch (IOException e) {
            logger.error("Declare a queue occur error,the queue is {} and the error is {}", getName(), e);
        } finally {
            channelFactory.closeChannel(channel);
        }
    }


}
