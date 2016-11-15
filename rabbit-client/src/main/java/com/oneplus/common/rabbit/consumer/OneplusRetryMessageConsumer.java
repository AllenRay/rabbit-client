package com.oneplus.common.rabbit.consumer;

import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.exception.RabbitMessageReceiveException;
import com.oneplus.common.rabbit.handler.Handler;
import com.oneplus.common.rabbit.handler.HandlerService;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by allen lei on 2016/2/24.
 * simple consumer,
 * will use spring retry to support retry.
 */
public class OneplusRetryMessageConsumer extends OnePlusDefaultMessageConsumer {

    private final Logger logger = LoggerFactory.getLogger("#Message_Consumer#");

    private final RetryTemplate retryTemplate = new RetryTemplate();

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public OneplusRetryMessageConsumer(Channel channel, HandlerService handlerService, MessageConverter messageConverter, String queue) {
        super(channel, handlerService, messageConverter, queue);

        initRetryPolicy();
    }

    /**
     * init retry policy.
     */
    private void initRetryPolicy() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setMultiplier(4.0d);
        backOffPolicy.setInitialInterval(3000L);
        backOffPolicy.setMaxInterval(60000L);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        //only retry runtime exception.
        Map<Class<? extends Throwable>, Boolean> exceptionsRetryMap = new HashMap<>();
        exceptionsRetryMap.put(RuntimeException.class, true);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsRetryMap);
        retryTemplate.setRetryPolicy(simpleRetryPolicy);
    }

    /**
     * use spring retry to retry failed message.
     *
     * @param message
     * @param handler
     * @return
     */
    @Override
    public boolean processMessage(final Message message, final Handler handler) {
        Boolean successful;
        try {
            successful = retryTemplate.execute(new RetryCallback<Boolean, RabbitMessageReceiveException>() {
                @Override
                public Boolean doWithRetry(RetryContext context) {
                    boolean successful;
                    try {
                        successful = handler.handleMessage(message);
                    } catch (Throwable throwable) {
                        throw new RabbitMessageReceiveException("Handler message occur error,will retry.", throwable);
                    }
                    //if message consumed failed,throw exception to retry.
                    if (!successful) {
                        throw new RabbitMessageReceiveException("Message consumed failed,will retry.");
                    }
                    return true;
                }
            }, new RecoveryCallback<Boolean>() {
                @Override
                public Boolean recover(RetryContext context) throws Exception {
                    return false;
                }
            });
        } catch (Throwable throwable) {
            return false;
        }
        return successful;
    }

}
