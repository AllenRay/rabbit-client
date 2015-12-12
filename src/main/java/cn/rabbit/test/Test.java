package cn.rabbit.test;


import cn.rabbit.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Administrator on 2015/12/7.
 */
public class Test {


    public static void main(String[] args) {


       ApplicationContext context = new ClassPathXmlApplicationContext("rabbitConfiguration.xml");
        RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate");

       rabbitTemplate.receive("oneplus-test2-7");


   }




}
