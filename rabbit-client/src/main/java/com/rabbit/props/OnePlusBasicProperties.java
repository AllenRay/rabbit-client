package com.rabbit.props;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.Map;

/**
 * Created by allen lei on 2016/8/9.
 * version 1.0.0
 * company oneplus
 */
public class OnePlusBasicProperties implements BasicProperties {

    private String contentType;
    private String contentEncoding;
    private Map<String, Object> headers;
    private Integer deliveryMode;
    private Integer priority;
    private String correlationId;
    private String replyTo;
    private String expiration;
    private String messageId;
    private Date timestamp;
    private String type;
    private String userId;
    private String appId;
    private String clusterId;

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public void setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public void setDeliveryMode(Integer deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public void setExpiration(String expiration) {
        this.expiration = expiration;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public String getContentType() {
        return this.contentType;
    }

    @Override
    public String getContentEncoding() {
        return this.contentEncoding;
    }

    @Override
    public Map<String, Object> getHeaders() {
        return this.headers;
    }

    @Override
    public Integer getDeliveryMode() {
        return this.deliveryMode;
    }

    @Override
    public Integer getPriority() {
        return this.priority;
    }

    @Override
    public String getCorrelationId() {
        return this.correlationId;
    }

    @Override
    public String getReplyTo() {
        return this.replyTo;
    }

    @Override
    public String getExpiration() {
        return this.expiration;
    }

    @Override
    public String getMessageId() {
        return this.messageId;
    }

    @Override
    public Date getTimestamp() {
        return this.timestamp;
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public String getUserId() {
        return this.userId;
    }

    @Override
    public String getAppId() {
        return this.appId;
    }

    /**
     * convert to AMQP basic properties.
     * @return
     */
    public AMQP.BasicProperties convertToBasicProperties(){
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
        if(!StringUtils.isEmpty(getAppId())){
            builder.appId(getAppId());
        }

        if (!StringUtils.isEmpty(getClusterId())){
            builder.clusterId(getClusterId());
        }

        if(!StringUtils.isEmpty(getContentEncoding())){
            builder.contentEncoding(getContentEncoding());
        }

        if(!StringUtils.isEmpty(getContentType())){
            builder.contentType(getContentType());
        }

        if(!StringUtils.isEmpty(getCorrelationId())){
            builder.correlationId(getCorrelationId());
        }

        if(getDeliveryMode() != null){
            builder.deliveryMode(getDeliveryMode());
        }

        if(!StringUtils.isEmpty(getExpiration())){
            builder.expiration(getExpiration());
        }

        if(!CollectionUtils.isEmpty(getHeaders())){
            builder.headers(getHeaders());
        }

        if(!StringUtils.isEmpty(getMessageId())){
            builder.messageId(getMessageId());
        }

        if(getPriority() != null){
            builder.priority(getPriority());
        }

        if(!StringUtils.isEmpty(getReplyTo())){
            builder.replyTo(getReplyTo());
        }

        if(getTimestamp() != null){
            builder.timestamp(getTimestamp());
        }

        if(!StringUtils.isEmpty(getType())){
            builder.type(getType());
        }

        if(!StringUtils.isEmpty(getUserId())){
            builder.userId(getUserId());
        }

        return builder.build();
    }
}
