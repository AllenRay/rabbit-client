package com.oneplus.common.rabbit.store;

import com.oneplus.common.rabbit.ChannelFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.awt.*;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by allen lei on 2016/8/8.
 * version 1.0.0
 * company oneplus
 * 根据message retriever，找到还未发送的message，再进行补偿发送。
 * 为了避免多个节点同时查询数据，发送重复的消息，只有得到锁的节点才能执行从数据库中查询并且发送消息
 * 分布式锁，将会使用zk
 */
public class MessageCompensater {

    private final Logger logger = LoggerFactory.getLogger(MessageCompensater.class);

    private final static String LOCK_PATH = "/ROOT/MESSAGE/LOCK";

    private MessageStoreAndRetriever messageStoreAndRetriever;

    private ChannelFactory channelFactory;

    private String zkConnect;

    private CuratorFramework client;

    private InterProcessMutex lock;

    public MessageCompensater(MessageStoreAndRetriever messageStoreAndRetriever,
                              ChannelFactory channelFactory, String zkConnect) {
        this.messageStoreAndRetriever = messageStoreAndRetriever;
        this.channelFactory = channelFactory;
        this.zkConnect = zkConnect;
    }

    //初始化一个线程，每一分钟跑一次
    private ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(1);

    public void start() {
        logger.info("Starting retrieve unsend messages..");

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy);
        if (client == null) {
            throw new IllegalStateException("The zookeeper client is null.");
        }
        //start curator client to connect zk.
        client.start();

        //instance distrbuted lock.
        lock = new InterProcessMutex(client,LOCK_PATH);

        //submit task.
        executorService.scheduleWithFixedDelay(new RetrieveMessagesToSend(),100,60,TimeUnit.SECONDS);


    }

    public void stop(){
        if(executorService != null){
            executorService.shutdown();
        }

        if(client != null){
            client.close();
        }

    }

    private class RetrieveMessagesToSend implements Runnable {

        @Override
        public void run() {
            //为了避免多个节点，同时跑任务，而把同一个消息重复发送多次，所以job在真正运行之前,每个节点去获取锁
            //只有获得锁的节点才能够跑任务
            try {
                boolean locked = lock.acquire(5,TimeUnit.SECONDS);
                try {
                    if(locked) {
                        logger.info("Get lock and start to retrieve messages and to send.");
                        retrieveMessageAndSend();
                    }
                } finally {
                    if(locked) {
                        lock.release();
                    }
                }
            }catch (Exception e){
                logger.error("Acquired or release lock occur error."+e);
            }


        }

    }

    private void retrieveMessageAndSend() {
        List<MessageStoreBean> messageStoreBeans = messageStoreAndRetriever.retrieveUnSendMessages();
        if (!CollectionUtils.isEmpty(messageStoreBeans)) {
            logger.info("Find UNSend Messages,size is {}", messageStoreBeans.size());
            Channel channel = channelFactory.getProducerChannel();
            try {
                for (MessageStoreBean storeBean : messageStoreBeans) {
                    String exchange = storeBean.getExchange();
                    String routingKey = storeBean.getRoutingKey();
                    AMQP.BasicProperties basicProperties = storeBean.getBasicProperties();
                    byte[] payload = storeBean.getPayload();
                    final String messageKey = storeBean.getMessageKey();

                    channel.addConfirmListener(new ConfirmListener() {
                        @Override
                        public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                            if (!StringUtils.isEmpty(messageKey))
                                logger.info("Message compensater have re pulished message {} and remove it.", messageKey);
                            messageStoreAndRetriever.removeMessage(messageKey);
                        }

                        @Override
                        public void handleNack(long deliveryTag, boolean multiple) throws IOException {

                        }
                    });

                    channel.confirmSelect();
                    channel.basicPublish(exchange, StringUtils.isEmpty(routingKey) ? "" : routingKey, basicProperties, payload);
                    channel.waitForConfirms(3000l);

                    //clear channel listener.
                    //to avoid have been confirmed message confirm again.
                    channel.clearConfirmListeners();


                }

            } catch (Throwable e) {
                logger.error("Compensate messsge occur error." + e);
            } finally {
                channelFactory.returnProducerChannel(channel);
            }
        }
    }




}
