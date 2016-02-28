package com.oneplus.common.rabbit;

import com.oneplus.common.log.LogUtils;
import com.oneplus.common.rabbit.utils.MessageUtils;

import java.io.Serializable;

/**
 * Created by Allen lei on 2015/12/9.
 * 封装消息的对象。
 */
public class Message implements Serializable{

    private String messageId;
    private String messageName;
    private long   deliveryTag;
    private String customerTag;
    private String requestId;
    private Object messageBody;
    private boolean ack;


    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageName() {
        return messageName;
    }

    public void setMessageName(String messageName) {
        this.messageName = messageName;
    }

    public Object getMessageBody() {
        return messageBody;
    }

    public void setMessageBody(Object messageBody) {
        this.messageBody = messageBody;
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(long deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getCustomerTag() {
        return customerTag;
    }

    public void setCustomerTag(String customerTag) {
        this.customerTag = customerTag;
    }

    public boolean isAck() {
        return ack;
    }

    public void setAck(boolean ack) {
        this.ack = ack;
    }

    public static com.oneplus.common.rabbit.Message build(){
        com.oneplus.common.rabbit.Message message = new com.oneplus.common.rabbit.Message();
        message.setMessageId(MessageUtils.generateShortMessageId());
        message.setRequestId(LogUtils.getLogId());
        return  message;
    }

    public com.oneplus.common.rabbit.Message messageBody(Object messageBody){
        setMessageBody(messageBody);
        return this;
    }

    public com.oneplus.common.rabbit.Message ack(Boolean ack){
        setAck(ack);
        return this;
    }

    public com.oneplus.common.rabbit.Message messageName(String messageName){
        setMessageName(messageName);
        return this;
    }

    public com.oneplus.common.rabbit.Message customerTag(String customerTag){
        setCustomerTag(customerTag);
        return this;
    }

    public com.oneplus.common.rabbit.Message deliveryTag(Long deliveryTag){
        setDeliveryTag(deliveryTag);
        return this;
    }


}
