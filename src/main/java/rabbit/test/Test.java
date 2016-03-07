package rabbit.test;

import com.rabbitmq.client.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import rabbit.ChannelFactory;
import rabbit.Message;
import rabbit.RabbitTemplate;
import rabbit.utils.MessageUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Administrator on 2016/3/3.
 */
public class Test {

    public static void main(String[] args) throws Exception{
        ApplicationContext context = new ClassPathXmlApplicationContext("application.xml");

        RabbitTemplate rabbitTemplate = context.getBean("rabbitTemplate", RabbitTemplate.class);

        /*Message message = new Message();
        message.setMessageId(MessageUtils.generateShortMessageId());

        rabbitTemplate.sendMessage(message,"oneplus_exchange","cn.oneplus.message");*/

        //rabbitTemplate.receive("oneplus_test_deadletter");

        rabbitTemplate.receiveMessageWithDelay("oneplus_queue");




    }
}
