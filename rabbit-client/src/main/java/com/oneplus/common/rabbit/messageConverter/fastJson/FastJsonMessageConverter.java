package com.oneplus.common.rabbit.messageConverter.fastJson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.oneplus.common.rabbit.Message;
import com.oneplus.common.rabbit.messageConverter.MessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Created by allen lei on 2016/8/11.
 * version 1.0.0
 * company oneplus
 */
public class FastJsonMessageConverter implements MessageConverter {

    private static final Logger logger = LoggerFactory.getLogger(FastJsonMessageConverter.class);

    @Override
    public byte[] convertToMessage(Object t) {
        return JSON.toJSONBytes(t,SerializerFeature.WriteClassName);
    }

    @Override
    public Object convertToObject(byte[] messages) {
        try {
            String str = new String(messages,"utf-8");
            return JSON.parseObject(str,Message.class);
        } catch (UnsupportedEncodingException e) {
            logger.error("cant support utf-8");
        }
        return null;
    }
}
