package cn.rabbit.test;

import cn.rabbit.Message;
import cn.rabbit.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Administrator on 2015/12/10.
 */
public class ProducterTest {

    public static void main(String[] args) {


        ApplicationContext context = new ClassPathXmlApplicationContext("rabbitConfiguration.xml");
        RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate");

        Email email = new Email();
        email.setEmailContent("test");
        email.setEmailTempate("11111");
        Message message = new Message();
        message.setMessageBody(email);

        rabbitTemplate.sendMessage(message,"test-2","test-2");

        /*byte [] bytes = rabbitTemplate.getMessageConverter().convertToMessage(message);

        Object o = rabbitTemplate.getMessageConverter().convertToObject(bytes);*/

        rabbitTemplate.getChannelFactory().closeConnection();

    }
}
