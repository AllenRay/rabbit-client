package com.rabbit.consumer;

import com.rabbit.Message;
import com.rabbit.handler.Handler;
import com.rabbit.handler.HandlerService;
import com.rabbit.lyra.internal.util.Assert;
import com.rabbit.messageConverter.MessageConverter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by allen lei on 2016/5/27.
 * version 1.0.0
 * company oneplus
 */
public abstract class OnePlusDefaultMessageConsumer extends DefaultConsumer {

    private final Logger logger = LoggerFactory.getLogger("#Message_Consumer#");

    private MessageConverter messageConverter;

    private HandlerService handlerService;

    private String queue;

    private ThreadPoolExecutor executor;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public OnePlusDefaultMessageConsumer(Channel channel, HandlerService handlerService, MessageConverter messageConverter,
                                         String queue,ThreadPoolExecutor executor) {
        super(channel);

        Assert.notNull(handlerService);
        Assert.notNull(messageConverter);
        Assert.notNull(queue);

        this.handlerService = handlerService;
        this.messageConverter = messageConverter;
        this.queue = queue;

        this.executor = executor;

    }


    public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {

        //async process message.
        executor.submit(new Runnable() {
            public void run() {
                try {
                    Object message;
                    try {
                        message = messageConverter.convertToObject(body);
                    } catch (Throwable e) {
                        logger.error("Convert message occur error,the error is:" + e);
                        getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                        return;
                    }

                    if (message == null) {
                        getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                        return;
                    }
                    if (message instanceof Message) {
                        final Message messageBody = (Message) message;
                        final Handler handler = handlerService.getConsumerHandler(queue);

                        if (handler == null) {
                            logger.error("No handler for this message {},so stop handle it and nack.", messageBody.getMessageId());
                            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                            return;
                        }
                        MDC.put("oneplusLogId",messageBody.getRequestId());
                        logger.info("{} message {} process",messageBody.getRequestId(),((Message) message).getMessageId());
                        boolean successful;
                        try {
                            successful = processMessage(messageBody, handler);
                        } catch (Throwable e) {
                            //catch all exception and nack message, if throw any runtime exception,the channel will be closed.
                            logger.error("Handler message occur error", e);
                            if (getChannel() != null) {
                                getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                            }
                            return;
                        }finally {
                            MDC.remove("oneplusLogId");
                        }
                        logger.info("Message {} handler result is {}", messageBody.getMessageId(), successful);

                        if (successful) {
                            getChannel().basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                        }

                    } else {
                        getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                        logger.error("invalid message,the valid message type should be {} but current message type is {}", Message.class.getName(),
                                message != null ? message.getClass().getName() : "null");
                    }
                } catch (Throwable e) {
                    logger.error("Process error occur error." + e);
                }
            }

        });
    }

    public abstract boolean processMessage(Message message, Handler handler);

    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    public HandlerService getHandlerService() {
        return handlerService;
    }

    public void setHandlerService(HandlerService handlerService) {
        this.handlerService = handlerService;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }
}
