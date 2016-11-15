package com.oneplus.common.rabbit;

import java.util.Map;

/**
 * Created by allen lei on 2015/12/8.
 */
public class QueueBind{

    private String queue;
    private String exchange;
    private String routingKey = "";
    private Map<String,Object> arguments;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
