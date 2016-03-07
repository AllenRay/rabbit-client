package rabbit.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.ChannelFactory;
import rabbit.Message;
import rabbit.RabbitTemplate;
import rabbit.config.DelayQueueConfig;
import rabbit.handler.Handler;
import rabbit.handler.HandlerService;
import rabbit.lyra.internal.util.Assert;
import rabbit.messageConverter.MessageConverter;
import rabbit.utils.Constants;
import rabbit.utils.MessagePropertiesBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by allen lei on 2016/3/3.
 */
public class DelayMessageConsumer extends DefaultConsumer{

    private final Logger logger = LoggerFactory.getLogger("#DeadLetter_Message_Consumer#");

    private MessageConverter messageConverter;

    private HandlerService handlerService;

    private String queue;

    private RabbitTemplate rabbitTemplate;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public DelayMessageConsumer(Channel channel, HandlerService handlerService, MessageConverter messageConverter,String queue,RabbitTemplate rabbitTemplate) {
        super(channel);

        Assert.notNull(handlerService);
        Assert.notNull(messageConverter);
        Assert.notNull(queue);
        Assert.notNull(rabbitTemplate);

        this.handlerService = handlerService;
        this.messageConverter = messageConverter;
        this.queue = queue;
        this.rabbitTemplate = rabbitTemplate;

    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        final Object message = this.messageConverter.convertToObject(body);
        if (message instanceof Message) {
            final Message messageBody = (Message) message;
            Map<String,Object> header = properties.getHeaders();


            final Handler handler = handlerService.getConsumerHandler(queue);
            if(handler == null){
                logger.error("No handler for this message {},so stop handle it and nack.",messageBody.getMessageId());
                getChannel().basicNack(envelope.getDeliveryTag(),false,false);
                return;
            }

            boolean successful = false;
            try {
                successful = handler.handleMessage(messageBody);
            }catch (Throwable e){
                //catch all exception and nack message, if throw any runtime exception,the channel will be closed.
                logger.error("Handler message occur error,the error is {} ",e);
                if(getChannel() != null){
                    getChannel().basicNack(envelope.getDeliveryTag(),false,false);
                }
            }
            logger.info("Message {} handler result is {}",messageBody.getMessageId(),successful);

            if (successful) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                return;
            }


            DelayQueueConfig queueConfig = this.handlerService.getQueueConfig(queue);
            if(queueConfig == null) {
                getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                logger.info("This queue {} message has no delay and retry config, so no ack this message {} ",queue,messageBody.getMessageId());
                return;
            }


            //get try count from message header.
            int count;
            long delay;
            Integer tryCount = header != null ? (Integer)header.get(Constants.TRY_COUNT) : null;
            if(tryCount == null){
                count = 1;
                delay = queueConfig.getInitDelay();
            }else {
                 //return after reach max try count.
                 if(tryCount >= queueConfig.getMaxTry()){
                     getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                     return;
                 }
                 count = tryCount.intValue()+1;
                 BigDecimal bigDecimal = new BigDecimal(queueConfig.getMultiplier() * queueConfig.getInitDelay() * tryCount.intValue());
                 delay = bigDecimal.longValue();
            }

            //retry message.
            Map<String,Object> maps = new HashMap<>();
            maps.put(Constants.DELAY,delay);
            maps.put(Constants.TRY_COUNT,count);
            AMQP.BasicProperties basicProperties = MessagePropertiesBuilder.buildPropertiesWithHeader(maps);

            rabbitTemplate.sendMessage(messageBody,this.handlerService.getDelayExchangeName(),queueConfig.getRoutingKey(),basicProperties);

            //to avoid message lost, no ack message after delay message send successful.
            getChannel().basicNack(envelope.getDeliveryTag(), false, false);


        } else {
            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
            if (logger.isErrorEnabled()) {
                logger.error("invalid message,the valid message type should be {} but current message type is {}", Message.class.getName(),
                        message != null ? message.getClass().getName() : "null");
            }
        }
    }





}
