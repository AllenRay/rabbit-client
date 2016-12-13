package com.rabbit.store;

import java.util.List;

/**
 * Created by allen lei on 2016/8/8.
 * version 1.0.0
 * company oneplus
 */
public interface MessageRetriever {

    /**
     * 查询出，还未发送成功的消息。
     *  1，尽量避免查询出和当前时间还没超过1秒的消息，有可能还没ACK
     * @return
     */
    List<MessageStoreBean> retrieveUnSendMessages();

    /**
     * retrieve all compensation message count
     * @return
     */
    int retrieveMessageTotalCount();

    /**
     * get compensation time.
     *
     * @return
     */
    long getCompensationTime();
}
