package com.rabbit.store;


import java.util.List;

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
    Integer storeMessage(MessageStoreBean messageStoreBean);

    /**
     * 根据message key删除消息
     *
     * @param messageKey 其实是 message Id
     */
    void removeMessage(String messageKey);

    /**
     * 根据message id删除message
     * @param messageId
     */
    void removeMessageById(int messageId);

    /**
     * batch remove messages.
     * @param messageKeys
     */
    void batchRemoveMessages(List<String> messageKeys);

    /**
     * batch remove ids.
     * @param messageIds
     */
    void batchRemoveMessageIds(List<Integer> messageIds);

}
