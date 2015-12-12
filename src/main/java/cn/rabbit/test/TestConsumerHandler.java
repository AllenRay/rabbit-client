package cn.rabbit.test;

import cn.rabbit.Message;
import cn.rabbit.handler.ConsumerHandler;

/**
 * Created by Administrator on 2015/12/10.
 */
@cn.rabbit.ConsumerHandler(queue = "${queue}")
public class TestConsumerHandler extends ConsumerHandler {

    @Override
    public boolean handle(Message message) {
        System.out.println("message received: "+message.getDeliveryTag());
        return true;
    }

}
