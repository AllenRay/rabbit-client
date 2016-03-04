package rabbit.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import rabbit.Message;
import rabbit.handler.Handler;
import rabbit.handler.HandlerService;
import rabbit.lyra.internal.util.Assert;
import rabbit.messageConverter.MessageConverter;
import rabbit.utils.Constants;

import java.io.IOException;
import java.util.Map;

/**
 * Created by allen lei on 2016/3/3.
 */
public class DelayMessageConsumer extends DefaultConsumer{

    private final Logger logger = LoggerFactory.getLogger("#DeadLetter_Message_Consumer#");

    private MessageConverter messageConverter;

    private HandlerService handlerService;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public DelayMessageConsumer(Channel channel, HandlerService handlerService, MessageConverter messageConverter) {
        super(channel);

        Assert.notNull(handlerService);
        Assert.notNull(messageConverter);

        this.handlerService = handlerService;
        this.messageConverter = messageConverter;
    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        final Object message = this.messageConverter.convertToObject(body);
        if (message instanceof Message) {
            final Message messageBody = (Message) message;
            Map<String,Object> header = properties.getHeaders();
            String queue = (String)header.get(Constants.QUEUE);


            if(StringUtils.isEmpty(queue)){
                logger.error("Don't know current message {} belong to which queue,so stop process this message and discard it.",messageBody.getMessageId());
                getChannel().basicNack(envelope.getDeliveryTag(),false,false);
                return;
            }

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

            //getChannel().basicNack(envelope.getDeliveryTag(), false, false);


        } else {
            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
            if (logger.isErrorEnabled()) {
                logger.error("invalid message,the valid message type should be {} but current message type is {}", Message.class.getName(),
                        message != null ? message.getClass().getName() : "null");
            }
        }
    }





}
