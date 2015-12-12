package cn.rabbit.test;

import cn.rabbit.handler.Handler;
import cn.rabbit.handler.HandlerService;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2015/12/12.
 */
public class TestPattern {


    public static void main(String[] args) {
        Map<String,Handler> handlerMap = new HashMap<String, Handler>();
        handlerMap.put("test-1:a.*",new TestProducerHandler());
        //handlerMap.put("test-1:*.a",new TestProducerHandler());
        //handlerMap.put("test-1:a.#",new TestProducerHandler());
        handlerMap.put("test-1:#.a",new TestProducerHandler());
        handlerMap.put("test-1:aa",new TestProducerHandler());

        HandlerService service = new HandlerService();
        service.setHandlerMap(handlerMap);

        Handler handler1 = service.getProducerHandler("test-1", "aa");
        Assert.notNull(handler1, "can't find handler");

        Handler handler2 = service.getProducerHandler("test-1","c.d.a");
        Assert.notNull(handler2,"can't find handler");


    }


}
