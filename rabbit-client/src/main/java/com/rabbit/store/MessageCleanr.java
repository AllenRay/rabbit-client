package com.rabbit.store;

import com.rabbit.common.LocalhostService;
import com.rabbit.lyra.internal.util.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by leizhengyu on 11/24/16.
 * <p>
 * async remove messages
 */
public class MessageCleanr {

    private final static String REMOVE = "remove";

    private final Logger logger = LoggerFactory.getLogger(MessageCleanr.class);

    private MessageStore messageStore;

    private boolean running = true;

    private JedisPool jedisPool;

    private String appName;

    private LocalhostService localhostService = new LocalhostService();

    private String ip = "127.0.0.1";

    private String removeListName;


    private final ThreadPoolExecutor workerGroup = new ThreadPoolExecutor(5, 8, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(200), new NamedThreadFactory("JDBC-Message-clean-worker-%s"), new ThreadPoolExecutor.AbortPolicy());

    private final ScheduledExecutorService bossGroup = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JDBC-Message-clean-boss-%s"));


    @PostConstruct
    public void start() {
        ip = localhostService.getIp();

        removeListName = REMOVE + "-" + appName + "-" + ip;
        //start clean.
        bossGroup.scheduleWithFixedDelay(new Bosser(removeListName), 10, 30, TimeUnit.MILLISECONDS);


    }


    private class Bosser implements Runnable {

        private String key;

        private Bosser(String key) {
            this.key = key;
        }

        @Override
        public void run() {
            try (Jedis jedis = jedisPool.getResource()) {
                Set<String> removeIds = jedis.smembers(key);//get all

                if (removeIds == null || removeIds.isEmpty()) {
                    return;
                }
                jedis.srem(key, removeIds.toArray(new String[removeIds.size()]));

                List<Integer> shouldRemoveIds = new ArrayList<>();
                for (String id : removeIds) {
                    shouldRemoveIds.add(Integer.parseInt(id));
                }

                workerGroup.submit(new Worker(shouldRemoveIds));
            } catch (Throwable e) {
                logger.warn("Message clean thread stoped when occured error." + e);
            }

        }
    }

    private class Worker implements Runnable {

        private List<Integer> shouldRemoveIds;

        public Worker(List<Integer> shouldRemoveIds) {
            this.shouldRemoveIds = shouldRemoveIds;
        }

        @Override
        public void run() {
            messageStore.batchRemoveMessageIds(shouldRemoveIds);
        }
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
