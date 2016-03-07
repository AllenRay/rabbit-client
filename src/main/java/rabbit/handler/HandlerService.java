package  rabbit.handler;

import org.springframework.beans.factory.annotation.Value;
import  rabbit.ConsumerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;
import rabbit.config.DelayQueueConfig;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Allen lei on 2015/12/12.
 * 解析所有的handler，并且根据特定的规则去寻找ProducerHandler/ConsumerHandler
 */
public class HandlerService implements ApplicationContextAware,EmbeddedValueResolverAware {

    private final Logger logger = LoggerFactory.getLogger( rabbit.handler.HandlerService.class);

    private Map<String,  rabbit.handler.Handler> handlerMap = new HashMap<>();

    private Map<String,DelayQueueConfig> delayQueueConfigMap = new HashMap<>();

    private ApplicationContext context;

    private StringValueResolver stringValueResolver;

    @Value("${delay_exchange_name}")
    private String delayExchangeName;

    /**
     * init handler map, find all handler from context.
     *
     */
    @PostConstruct
    public void initHandlerMap(){
        //从context中寻找到当前所有的handler
        Map<String,  rabbit.handler.Handler> map = BeanFactoryUtils.beansOfTypeIncludingAncestors(context,  rabbit.handler.Handler.class, true, false);

        if(map != null && !map.isEmpty()) {
            for ( rabbit.handler.Handler handler : map.values()) {
                //根据注解去匹配是producer handler 还是 consumer handler
                 rabbit.ProducerHandler producerHandler = AnnotationUtils.findAnnotation(handler.getClass(),  rabbit.ProducerHandler.class);
                if (producerHandler != null) {
                    String exchange = producerHandler.exchange();
                    if(StringUtils.isEmpty(exchange)){
                        logger.error("This producer handler will be ignored,because has not set exchange value.");
                        continue;
                    }
                    String resolveExchange = stringValueResolver.resolveStringValue(exchange);
                    String routingKey = producerHandler.routingKey();
                    String key;

                    if(StringUtils.isEmpty(routingKey)){
                        key = resolveExchange;
                    }else {
                        String resolveRoutingKey = stringValueResolver.resolveStringValue(routingKey);
                        key = resolveExchange + ":" + resolveRoutingKey;
                    }

                    this.handlerMap.put(key, handler);
                } else {
                     rabbit.ConsumerHandler consumerHandler = AnnotationUtils.findAnnotation(handler.getClass(), ConsumerHandler.class);
                    if (consumerHandler != null) {
                        String queue = consumerHandler.queue();
                        if (StringUtils.isEmpty(queue)) {
                            logger.error("This consumer handler will be ignored,because has no set queue value.");
                            continue;
                        }
                        String resolveQueue = stringValueResolver.resolveStringValue(queue);
                        this.handlerMap.put(resolveQueue, handler);
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
    public  rabbit.handler.Handler getProducerHandler(String exchange,String routingKey){
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

    /**
     * get delay queue config.
     * @param queue
     * @return
     */
    public DelayQueueConfig getQueueConfig(String queue){
        if(this.delayQueueConfigMap.isEmpty()){
            return null;
        }
        if(!this.delayQueueConfigMap.containsKey(queue)){
            return null;
        }

        return this.delayQueueConfigMap.get(queue);
    }

    public Map<String, DelayQueueConfig> getDelayQueueConfigMap() {
        return delayQueueConfigMap;
    }

    public void setDelayQueueConfigMap(Map<String, DelayQueueConfig> delayQueueConfigMap) {
        this.delayQueueConfigMap = delayQueueConfigMap;
    }

    public String getDelayExchangeName() {
        return delayExchangeName;
    }

    public void setDelayExchangeName(String delayExchangeName) {
        this.delayExchangeName = delayExchangeName;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    public ApplicationContext getContext() {
        return context;
    }



    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
     this.stringValueResolver = resolver;
    }
}
