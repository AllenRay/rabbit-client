package com.rabbit.producer.compensation;

/**
 * Created by allen lei on 2016/8/9.
 * version 1.0.0
 * company oneplus
 */
public interface CompensateProducer {

    /**
     * 发送有可能需要补偿的消息
     *
     */
    void sendCompensationMessage(CompensationMessage compensationMessage);

}
