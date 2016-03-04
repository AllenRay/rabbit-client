package rabbit.test;

import com.rabbitmq.client.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import rabbit.ChannelFactory;
import rabbit.Message;
import rabbit.RabbitTemplate;
import rabbit.utils.MessageUtils;

import java.io.IOException;

/**
 * Created by Administrator on 2016/3/3.
 */
public class Test {

    public static void main(String[] args) throws Exception{
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");

        RabbitTemplate rabbitTemplate = context.getBean("rabbitTemplate", RabbitTemplate.class);

        //Message message = new Message();
        //message.setMessageId(MessageUtils.generateShortMessageId());

        //rabbitTemplate.sendMessage(message,"oneplus_exchange","oneplus.test.dead");

        //rabbitTemplate.receive("oneplus_test_deadletter");

        ChannelFactory channelFactory = context.getBean(ChannelFactory.class);

        Connection connection = channelFactory.getConnection();

        final Channel channel = connection.createChannel();

        channel.basicQos(0);
        channel.basicConsume("federation-queue",false,new DefaultConsumer(channel){
            /**
             * No-op implementation of {@link Consumer#handleDelivery}.
             */
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException
            {
                System.out.println(envelope.getDeliveryTag());
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        });


    }
}
