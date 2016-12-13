package com.rabbit.producer.compensation;

import com.rabbit.ChannelFactory;
import com.rabbit.Message;
import com.rabbit.common.DefaultBatchQueue;
import com.rabbit.common.LocalhostService;
import com.rabbit.common.MessageProducerSync;
import com.rabbit.lyra.internal.util.concurrent.NamedThreadFactory;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by allen lei on 2016/12/1.
 * version 1.0.0
 * company oneplus
 */
public class CompensateManager {

    private final Logger logger = LoggerFactory.getLogger(CompensateManager.class);

    private final static String REMOVE = "remove";

    //worker threads
    protected final ThreadPoolExecutor workerGroup = new ThreadPoolExecutor(10, 20, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000),
            new NamedThreadFactory("Rabbit-producer-worker-%s"), new ThreadPoolExecutor.AbortPolicy());

    //single boss thread.
    private final ExecutorService bossGroup = Executors.newSingleThreadExecutor(new NamedThreadFactory("Rabbit-producer-boss-%s"));

    /**
     * default queue.
     */
    private final DefaultBatchQueue<CompensationMessage> batchQueue = new DefaultBatchQueue<>(1024*64);

    private final MessageProducerSync messageProducerSync = new MessageProducerSync();

    private final LocalhostService localhostService = new LocalhostService();

    private boolean running = true;

    private final ChannelFactory channelFactory;

    private final JedisPool writeJedisPool;

    private final MessageConverter messageConverter;

    private final String appName;

    private String messageMapName;

    private String removeListName;

    private String ip = "127.0.0.1";

    public CompensateManager(ChannelFactory channelFactory,JedisPool writeJedisPool,
                             MessageConverter messageConverter,String appName){
        this.channelFactory = channelFactory;
        this.writeJedisPool = writeJedisPool;
        this.messageConverter = messageConverter;
        this.appName = appName;

        ip = localhostService.getIp();
        messageMapName = appName + "-" + ip;
        removeListName = REMOVE + "-" + messageMapName;

        //start
        bossGroup.submit(new BatchBosser());
    }


    public boolean putCompensationMessageToQueue(CompensationMessage message) throws InterruptedException {

        //如果3毫秒内没有put到内存队列，那么这个消息发送失败
        boolean offered = batchQueue.offer(message, 3, TimeUnit.MILLISECONDS);
        if (offered) {
            //logger.debug("put message {} to queue", JSON.toJSONString(message));
            //if message arrived,  signal.
            messageProducerSync.signalNotEmpty();
        }
        return offered;
    }

    private class BatchBosser implements Runnable {

        private List<CompensationMessage> lists = new ArrayList<>(100);

        private int emptyCount = 0;

        @Override
        public void run() {
            //looping always.
            //long remaining = 100;
            while (running) {

                try {
                    //await when three batch is empty.
                    //wait message arrived.
                    if (emptyCount >= 3) {
                        emptyCount = 0;
                        logger.debug("Message producer thread await message arrived.");
                        messageProducerSync.awaitWhenEmpty();
                    }

                    lists = CompensateManager.this.batchQueue.batch(100,100);
                } catch (InterruptedException e) {
                    logger.error("error"+e);
                }

                if (lists.isEmpty()) {
                    emptyCount++;
                    continue;
                }

                List<CompensationMessage> copys = new ArrayList<>(lists); //memory issue?
                lists.clear();

                logger.debug("Submit message {} to worker ",copys.size());

                workerGroup.submit(new BatchWorker(copys));
            }


        }
    }

    protected class BatchWorker implements Runnable {

        private List<CompensationMessage> compensationMessages;

        protected BatchWorker(List<CompensationMessage> compensationMessages) {
            this.compensationMessages = compensationMessages;
        }

        @Override
        public void run() {
            logger.debug("This batch size is {}", compensationMessages.size());
            Channel channel = channelFactory.getProducerChannel();
            try {
                long start = System.currentTimeMillis();

                channel.confirmSelect();// if the channel have open confirm select? need reopen?
                channel.addConfirmListener(new ConfirmListener() {
                    @Override
                    public void handleAck(long deliveryTag, boolean multiple) throws IOException {

                        List<String> shouldRemoveMessageIds = new ArrayList<>();
                        List<String> shouldRemoveMessageKeys = new ArrayList<>();

                        for (CompensationMessage message : compensationMessages) {
                            if (message.getMessageId() > 0) {
                                shouldRemoveMessageIds.add(String.valueOf(message.getMessageId()));
                            } else if (!StringUtils.isEmpty(message.getMessageKey())) {
                                shouldRemoveMessageKeys.add(message.getMessageKey());
                            }

                        }
                        try (Jedis jedis = writeJedisPool.getResource()) {
                            //将等待删除的message Id放入redis，等待清理线程进行清理。
                            if (!shouldRemoveMessageIds.isEmpty()) {
                                jedis.sadd(removeListName, shouldRemoveMessageIds.toArray(new String[shouldRemoveMessageIds.size()]));
                            }
                            //如果使用redis，就没必要通知清理线程去清理了。
                            if (!shouldRemoveMessageKeys.isEmpty()) {
                                jedis.hdel(messageMapName,shouldRemoveMessageKeys.toArray(new String[shouldRemoveMessageKeys.size()]));
                            }
                        }
                    }

                    @Override
                    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                        logger.warn("This batch messages have been rejected broker. the batch is {}", deliveryTag);
                    }
                });

                for (CompensationMessage compensationMessage : compensationMessages) {
                    String exchange = compensationMessage.getExchangeKey();
                    String routingKey = compensationMessage.getRoutingKey();
                    Message message = compensationMessage.getMessage();
                    AMQP.BasicProperties basicProperties = compensationMessage.getBasicProperties();

                    byte[] payload = messageConverter.convertToMessage(message);

                    channel.basicPublish(exchange, routingKey, basicProperties, payload);
                }
                channel.waitForConfirms();

                long end = System.currentTimeMillis();
                long spent = end - start;
                logger.info("Send compenstation message spent {} ms", spent);
            } catch (IOException | InterruptedException e) {
                logger.error("IOException occured,this batch message send failed." + e);
            } finally {
                channel.clearConfirmListeners();
                channelFactory.returnProducerChannel(channel);
            }
        }
    }




}
