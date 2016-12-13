package com.rabbit.producer.compensation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.rabbit.common.LocalhostService;
import com.rabbit.lyra.internal.util.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by leizhengyu on 11/29/16.
 */
public class RedisCompensateProducer extends DefaultCompensateProducer {

    private final Logger logger = LoggerFactory.getLogger(RedisCompensateProducer.class);

    private String ip;
    //redis map name. use appName +"-"+ip
    protected String messageMapName;

    protected String messageChannelName;

    private LocalhostService localhostService = new LocalhostService();

    private final JedisPool writeJedisPool;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor(
            new NamedThreadFactory("Redis-pubsub-%s"));

    public RedisCompensateProducer(CompensateManager compensateManager,String appName,JedisPool writeJedisPool) {
        super(compensateManager);

        this.writeJedisPool = writeJedisPool;

        ip = localhostService.getIp();
        messageMapName = appName + "-" + ip;
        messageChannelName = appName + ":" + ip;

        executorService.submit(new SubscriberThread());
    }

    @Override
    public void sendCompensationMessage(CompensationMessage compensationMessage) {
        try (Jedis jedis = writeJedisPool.getResource()) {
            String messageKey = UUID.randomUUID().toString();
            compensationMessage.setMessageKey(messageKey);
            String messageJson = JSON.toJSONString(compensationMessage, SerializerFeature.WriteClassName);
            //store and publish.
            jedis.hset(messageMapName, messageKey, messageJson);
            jedis.publish(messageChannelName,messageJson);

        }
    }

    private class CompensationMessageSub extends JedisPubSub{
        @Override
        public void onMessage(String channel, String message) {
            CompensationMessage compensationMessage = JSON.parseObject(message,CompensationMessage.class);
            RedisCompensateProducer.super.sendCompensationMessage(compensationMessage);
        }
    }


    private class SubscriberThread implements Runnable{

        @Override
        public void run() {
          try(Jedis jedis = writeJedisPool.getResource()){
              logger.info("Subscried channel is {}",messageChannelName);
              jedis.subscribe(new CompensationMessageSub(),messageChannelName);
          }
        }
    }
}
