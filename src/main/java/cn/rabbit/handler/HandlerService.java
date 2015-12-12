package cn.rabbit.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Allen lei on 2015/12/12.
 */
public class HandlerService {

    private final Logger logger = LoggerFactory.getLogger(HandlerService.class);

    public Map<String, Handler> getHandlerMap() {
        return handlerMap;
    }

    public void setHandlerMap(Map<String, Handler> handlerMap) {
        this.handlerMap = handlerMap;
    }

    private Map<String,Handler> handlerMap = new HashMap<String, Handler>();

    /**
     * init handler map, find all handler from context.
     *
     * @param context
     * @param stringValueResolver
     */
    public void initHandlerMap(ApplicationContext context,StringValueResolver stringValueResolver){
        //从context中寻找到当前所有的handler
        Map<String,Handler> map = BeanFactoryUtils.beansOfTypeIncludingAncestors(context, Handler.class, true, false);

        if(map != null && !map.isEmpty()) {
            for (Handler handler : map.values()) {
                //根据注解去匹配是producer handler 还是 consumer handler
                cn.rabbit.ProducerHandler producerHandler = AnnotationUtils.findAnnotation(handler.getClass(), cn.rabbit.ProducerHandler.class);
                if (producerHandler != null) {
                    String exchange = producerHandler.exchange();
                    if (exchange.startsWith("${") && exchange.endsWith("}")) {
                        exchange = stringValueResolver.resolveStringValue(exchange);
                    }
                    String routingKey = producerHandler.routingKey();
                    if (routingKey.startsWith("${") && routingKey.endsWith("}")) {
                        routingKey = stringValueResolver.resolveStringValue(routingKey);
                    }
                    if (StringUtils.isEmpty(exchange)) {
                        logger.error("This producer handler will be ignored,because has no set exchange value.");
                        continue;
                    }
                    String key = exchange + ":" + routingKey;
                    this.handlerMap.put(key, handler);
                } else {
                    cn.rabbit.ConsumerHandler consumerHandler = AnnotationUtils.findAnnotation(handler.getClass(), cn.rabbit.ConsumerHandler.class);
                    if (consumerHandler != null) {
                        String queue = consumerHandler.queue();
                        if (queue.startsWith("${") && queue.endsWith("}")) {
                            queue = stringValueResolver.resolveStringValue(queue);
                        }
                        if (StringUtils.isEmpty(queue)) {
                            logger.error("This consumer handler will be ignored,because has no set queue value.");
                            continue;
                        }
                        this.handlerMap.put(queue, handler);
                    } else {
                        logger.error("Don't know this handler type. should use annotation ProducerHandler/ConsumerHandler to marked");
                    }
                }
            }
        }
    }

    /**
     *
     * 支持匹配符，但不是正则表达式。
     * 只支持简单的表达式
     * 类似 a.*,*.a, a.#,#.a
     * * 表示一个单词
     * # 表示多个单词
     * @param exchange
     * @param routingKey
     * @return
     */
    public Handler getProducerHandler(String exchange,String routingKey){
        String key = exchange+":"+routingKey;
        if(this.handlerMap.containsKey(key)){
            return this.handlerMap.get(key);
        }

        //如果含有“.” 将会使用匹配模式
        if(!StringUtils.isEmpty(routingKey) && routingKey.indexOf(".") >= 0){
            //首先匹配下 a.* 或者 a.# 的模式
            String [] routingKeySplitByDot = routingKey.split("\\.");
            String tempRoutingKey;
            if(routingKeySplitByDot.length > 2){
                tempRoutingKey = routingKeySplitByDot[0]+".#";
            }else{
                tempRoutingKey = routingKeySplitByDot[0]+".*";
            }
            String adjustedKey = exchange+":"+tempRoutingKey;
            if(this.handlerMap.containsKey(adjustedKey)){
                return this.handlerMap.get(adjustedKey);
            }

            //再次匹配*.a #.a的模式
            if(routingKeySplitByDot.length > 2){
                tempRoutingKey = "#."+routingKeySplitByDot[routingKeySplitByDot.length-1];
            }else {
                tempRoutingKey = "*."+routingKeySplitByDot[routingKeySplitByDot.length-1];
            }
            adjustedKey = exchange+":"+tempRoutingKey;
            if(this.handlerMap.containsKey(adjustedKey)){
                return this.handlerMap.get(adjustedKey);
            }

        }
        return null;
    }

    /**
     * get consumer handler.
     * @param queue
     * @return
     */
    public Handler getConsumerHandler(String queue){
        if(this.handlerMap.containsKey(queue)){
            return this.handlerMap.get(queue);
        }

        return null;
    }




}
