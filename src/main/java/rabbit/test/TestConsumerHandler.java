package rabbit.test;


import rabbit.ConsumerHandler;
import rabbit.Message;

/**
 * Created by Administrator on 2016/3/3.
 */
@ConsumerHandler(queue = "oneplus_queue")
public class TestConsumerHandler extends rabbit.handler.ConsumerHandler {
    @Override
    public boolean handle(Message message) {
        return false;
    }
}
