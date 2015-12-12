package cn.rabbit.test;


import cn.rabbit.Message;
import cn.rabbit.handler.ProducerHandler;

/**
 * Created by Administrator on 2015/12/10.
 */
@cn.rabbit.ProducerHandler(exchange = "test-1")
public class TestProducerHandler extends ProducerHandler {

    @Override
    public void handle(Message message) {
        System.out.println("message send successful: "+message.getDeliveryTag());
    }

}
