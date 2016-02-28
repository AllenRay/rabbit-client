package com.oneplus.common.rabbit.retry;

import com.oneplus.common.rabbit.exception.RabbitMessageReceiveException;
import com.oneplus.common.rabbit.exception.RabbitMessageSendException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryState;
import org.springframework.retry.support.RetryTemplate;

/**
 * Created by allen lei on 2016/2/26.
 * oneplus retry template.
 */
public class OneplusRetryTemplate extends RetryTemplate {

    protected <T> T handleRetryExhausted(RecoveryCallback<T> recoveryCallback,
                                         RetryContext context, RetryState state) throws Throwable {
        Throwable throwable = context.getLastThrowable();
        if(throwable instanceof RabbitMessageReceiveException){
             return null;
        }
        if(throwable instanceof RabbitMessageSendException){
            return null;
        }
        return super.handleRetryExhausted(recoveryCallback,context,state);
    }
}
