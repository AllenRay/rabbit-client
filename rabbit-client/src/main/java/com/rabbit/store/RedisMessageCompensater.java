package com.rabbit.store;

import com.alibaba.fastjson.JSON;
import com.rabbit.ChannelFactory;
import com.rabbit.Message;
import com.rabbit.common.LocalhostService;
import com.rabbit.lyra.internal.util.concurrent.NamedThreadFactory;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbit.producer.compensation.CompensationMessage;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by allen lei on 2016/11/30.
 * version 1.0.0
 * company oneplus
 */
public class RedisMessageCompensater {

    private final Logger logger = LoggerFactory.getLogger(RedisMessageCompensater.class);

    private final LocalhostService localhostService = new LocalhostService();

    private String appName;

    private JedisPool jedisPool;

    private ChannelFactory channelFactory;

    private MessageConverter messageConverter;

    private ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(1,new NamedThreadFactory("Redis-compensation-%s"));


    public RedisMessageCompensater(String appName,JedisPool jedisPool,
                                    ChannelFactory channelFactory,MessageConverter messageConverter){
        this.appName = appName;
        this.jedisPool = jedisPool;
        this.channelFactory = channelFactory;
        this.messageConverter = messageConverter;

    }

    public void start(){
        executorService.scheduleAtFixedRate(new CompensationThread(),100,60, TimeUnit.SECONDS);

        //add shutdownhook to release resource.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                RedisMessageCompensater.this.executorService.shutdown();
            }
        });
    }

    private class CompensationThread implements Runnable{
        private String mapName;
        public CompensationThread(){
            mapName = appName + "-"+ localhostService.getIp();
        }

        @Override
        public void run() {
            try(Jedis jedis = jedisPool.getResource()){
                Map<String,String> maps = jedis.hgetAll(mapName);
                logger.info("Retrieve unsend message from redis");
                if(maps == null || maps.isEmpty()){
                    return;
                }

                Channel channel = channelFactory.getProducerChannel();
                try {
                    List<String> fields = new ArrayList<>();
                    for (Map.Entry<String, String> entry : maps.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        CompensationMessage compensationMessage = JSON.parseObject(value,CompensationMessage.class);
                        fields.add(key);
                        String exchangeKey = compensationMessage.getExchangeKey();
                        String routingKey = compensationMessage.getRoutingKey();
                        AMQP.BasicProperties basicProperties = compensationMessage.getBasicProperties();
                        Message message = compensationMessage.getMessage();

                        byte [] payload = messageConverter.convertToMessage(message);
                        channel.basicPublish(exchangeKey,routingKey,basicProperties,payload);
                    }
                    channel.waitForConfirms();
                    jedis.hdel(mapName,fields.toArray(new String[fields.size()]));
                }catch (Throwable e){
                    logger.warn("Send compensation message failed."+e);
                }finally {
                    channel.clearConfirmListeners();
                    channelFactory.returnProducerChannel(channel);
                }
            }
        }
    }





}
