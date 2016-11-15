package com.oneplus.common.rabbit.store;

import com.rabbitmq.client.AMQP;

/**
 * Created by allen lei on 2016/8/8.
 * version 1.0.0
 * company oneplus
 *
 * 用于在发送消息的时候，将消息存储在客户端，用于补偿
 */
public interface MessageStore {
    /**
     * 传入exchange ,routing key, basicProperties,以及payload.
     * payload 必须是序列化过后的内容，因为store只做最后的补偿，而不去做所谓的序列化
     *
     **/
    void storeMessage(MessageStoreBean messageStoreBean);

    /**
     * 根据message key删除消息
     * @param messageKey
     */
    void removeMessage(String messageKey);

}
