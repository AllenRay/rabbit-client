package rabbit.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import rabbit.Message;
import rabbit.exception.RabbitMessageReceiveException;
import rabbit.handler.Handler;
import rabbit.handler.HandlerService;
import rabbit.lyra.internal.util.Assert;
import rabbit.messageConverter.MessageConverter;
import rabbit.retry.SimpleRetryTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by allen lei on 2016/3/3.
 *
 * retry consumer.
 * use spring retry.
 */
public class RetryMessageConsumer extends DefaultConsumer{


    private final Logger logger = LoggerFactory.getLogger("#Retry_Message_Consumer#");

    private MessageConverter messageConverter;

    private HandlerService handlerService;

    private String queue;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public RetryMessageConsumer(Channel channel,HandlerService handlerService, MessageConverter messageConverter, String queue) {
        super(channel);

        Assert.notNull(handlerService);
        Assert.notNull(messageConverter);
        Assert.notNull(queue);
        this.handlerService = handlerService;
        this.messageConverter = messageConverter;
        this.queue = queue;
    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        final Object message = this.messageConverter.convertToObject(body);
        if (message instanceof Message) {
            final Message messageBody = (Message) message;
            final Handler handler = handlerService.getConsumerHandler(queue);

            if(handler == null){
                logger.error("No handler for this message {},so stop handle it and nack.",messageBody.getMessageId());
                getChannel().basicNack(envelope.getDeliveryTag(),false,false);
                return;
            }

            //will retry.
            RetryTemplate retryTemplate = new SimpleRetryTemplate();

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

            Boolean successful;
            try {
                successful = retryTemplate.execute(new RetryCallback<Boolean, RabbitMessageReceiveException>() {
                    @Override
                    public Boolean doWithRetry(RetryContext context) {
                        boolean successful;
                        try {
                            successful = handler.handleMessage(messageBody);
                        } catch (Throwable throwable) {
                            logger.error("Handler message {} occur error,the error is {}", messageBody.getMessageId(), throwable.getMessage());
                            throw new RabbitMessageReceiveException("Handler message occur error,will retry.");
                        }
                        //if message consumed failed,throw exception to retry.
                        if (!successful) {
                            logger.info("Message {} consumed failed,will retry.", messageBody.getMessageId());
                            throw new RabbitMessageReceiveException("Message consumed failed,will retry.");
                        }
                        return successful;
                    }
                });
            } catch (Throwable throwable) {
                if (logger.isErrorEnabled()) {
                    logger.error("Consuming message occur error,the error is: {}", throwable.getMessage());
                }
                //nack message
                getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                return;
            }


            if (successful != null && successful) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
            } else {
                getChannel().basicNack(envelope.getDeliveryTag(), false, false);
            }

        } else {
            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
            if (logger.isErrorEnabled()) {
                logger.error("invalid message,the valid message type should be {} but current message type is {}", Message.class.getName(),
                        message != null ? message.getClass().getName() : "null");
            }
        }
    }



}
