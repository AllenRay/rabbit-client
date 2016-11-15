package com.rabbit;


import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * Created by allen lei on 2015/12/11.
 * 使用此注解进行producer的handler的绑定。
 * 绑定的规则是exchange 和 routing key
 *
 * routing key 支持通配符格式
 * 比如
 *  a.* 将会匹配所有a.后面带一个单词
 *  a.# 匹配多个单词
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface ProducerHandler {

  String exchange() default "";

  String routingKey() default "";
}
