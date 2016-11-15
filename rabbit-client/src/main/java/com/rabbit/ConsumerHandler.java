package com.rabbit;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * Created by allen lei on 2015/12/11.
 * Consumer handler.
 * 将会使用queue来进行绑定
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface ConsumerHandler {
    String queue() default "";
}
