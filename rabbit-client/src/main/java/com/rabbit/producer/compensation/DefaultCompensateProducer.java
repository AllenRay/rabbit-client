package com.rabbit.producer.compensation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by allen lei on 2016/8/9.
 * version 1.0.0
 * company oneplus
 */
public class DefaultCompensateProducer implements CompensateProducer {


    private final static Logger logger = LoggerFactory.getLogger(DefaultCompensateProducer.class);

    private CompensateManager compensateManager;

    public DefaultCompensateProducer(CompensateManager compensateManager) {
        this.compensateManager = compensateManager;
    }

    @Override
    public void sendCompensationMessage(CompensationMessage compensationMessage) {
        try {
            this.compensateManager.putCompensationMessageToQueue(compensationMessage);
        } catch (InterruptedException e) {
            //ignore
        }
    }

}
