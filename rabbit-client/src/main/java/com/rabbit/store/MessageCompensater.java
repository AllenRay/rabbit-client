package com.rabbit.store;

import com.rabbit.ChannelFactory;
import com.rabbit.lyra.internal.util.Assert;
import com.rabbit.lyra.internal.util.concurrent.NamedThreadFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
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

    private String appName;

    private MessageStoreAndRetriever messageStoreAndRetriever;

    private ChannelFactory channelFactory;

    private String zkConnect;

    private CuratorFramework client;

    private InterProcessMutex lock;

    private String lockPath;

    public MessageCompensater(MessageStoreAndRetriever messageStoreAndRetriever,
                              ChannelFactory channelFactory, String zkConnect,
                              String appName) {
        this.messageStoreAndRetriever = messageStoreAndRetriever;
        this.channelFactory = channelFactory;
        this.zkConnect = zkConnect;
        this.appName = appName;

        lockPath = LOCK_PATH + "/" + appName;

        Assert.notNull(lockPath);


    }

    //初始化一个线程，每10秒钟尝试补偿一次
    private ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(1,new NamedThreadFactory("JDBC-compensation-%s"));

    public void start() {
        logger.info("Starting retrieve unSend messages..");

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy);
        if (client == null) {
            throw new IllegalStateException("The zookeeper client is null.");
        }
        //start curator client to connect zk.
        client.start();

        //instance distrbuted lock.
        lock = new InterProcessMutex(client,lockPath);

        //submit task.
        executorService.scheduleWithFixedDelay(new RetrieveMessagesToSend(),60,10,TimeUnit.SECONDS);

        //add shutdownhook to release resource.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                MessageCompensater.this.stop();
            }
        });


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
        int total = messageStoreAndRetriever.retrieveMessageTotalCount();
        int loopCount = total % 1000 == 0 ? total / 1000 : total / 1000 + 1;
        logger.info("Compensation count is {}",loopCount);
        for(int i = 0; i < loopCount ; i++) {
            List<MessageStoreBean> messageStoreBeans = messageStoreAndRetriever.retrieveUnSendMessages();
            if (!CollectionUtils.isEmpty(messageStoreBeans)) {
                Channel channel = channelFactory.getProducerChannel();
                try {
                    for (MessageStoreBean storeBean : messageStoreBeans) {
                        String exchange = storeBean.getExchange();
                        String routingKey = storeBean.getRoutingKey();
                        AMQP.BasicProperties basicProperties = storeBean.getBasicProperties();
                        byte[] payload = storeBean.getPayload();
                        final int messageId = storeBean.getMessageId();

                        channel.addConfirmListener(new ConfirmListener() {
                            @Override
                            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                                messageStoreAndRetriever.removeMessageById(messageId);
                            }

                            @Override
                            public void handleNack(long deliveryTag, boolean multiple) throws IOException {

                            }
                        });

                        channel.confirmSelect();
                        channel.basicPublish(exchange, routingKey, basicProperties, payload);
                        channel.waitForConfirms(3000l);

                        //clear channel listener.
                        //to avoid have been confirmed message confirm again.
                        channel.clearConfirmListeners();


                    }

                } catch (Throwable e) {
                    logger.error("Compensate message occur error." + e);
                } finally {
                    channelFactory.returnProducerChannel(channel);
                }
            }
        }
    }





}
