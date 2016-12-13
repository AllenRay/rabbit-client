package com.rabbit.common;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by leizhengyu on 11/30/16.
 */
public class MessageProducerSync {

    private final Lock elementLock = new ReentrantLock();

    private final Condition notEmpty = elementLock.newCondition();

    public void signalNotEmpty(){
        elementLock.lock();
        try {
            notEmpty.signal();
        }finally {
            elementLock.unlock();
        }
    }

    public void awaitWhenEmpty()throws InterruptedException{
        elementLock.lockInterruptibly();
        try{
            notEmpty.await();
        }finally {
            elementLock.unlock();
        }
    }
}
