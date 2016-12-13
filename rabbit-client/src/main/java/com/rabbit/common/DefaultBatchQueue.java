package com.rabbit.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by leizhengyu on 11/29/16.
 * 大部分逻辑都是blockingQueue,
 * 增加了batch消费的逻辑
 */
public class DefaultBatchQueue<E> extends  LinkedBlockingQueue<E> implements BatchQueue<E>{

    private final LinkedBlockingQueue<E> blockingQueue;


    public DefaultBatchQueue(){
        blockingQueue = new LinkedBlockingQueue<>(1024*10);
    }

    public DefaultBatchQueue(int capacity){
        if (capacity <= 0){
            throw new IllegalArgumentException("The capacity must be greater than 0");
        }
        blockingQueue = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public List<E> batch(int batchSize) throws InterruptedException{
        List<E> elements = new ArrayList<>(batchSize);
        do {
            //take and wait
            E e = take();
            if(e != null){
                elements.add(e);
            }
            if(elements.size() >= batchSize){
                break;
            }

        }while (true);

        return elements;
    }

    @Override
    public void put(E e) throws InterruptedException {
        blockingQueue.put(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return blockingQueue.offer(e, timeout, unit);
    }

    @Override
    public boolean offer(E e) {
        return blockingQueue.offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        return blockingQueue.take();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return blockingQueue.poll(timeout, unit);
    }

    @Override
    public E poll() {
        return blockingQueue.poll();
    }

    @Override
    public List<E> batch(int batchSize,long timeout) throws InterruptedException {
        List<E> elements = new ArrayList<>();
        long start = System.currentTimeMillis();
        long remaining = timeout;
        do {
            //poll and wait 1ms
            E e = poll(1,TimeUnit.MILLISECONDS);
            if(e != null){
                elements.add(e);
            }
            if(elements.size() >= batchSize){
                break;
            }
            long end = System.currentTimeMillis();
            long elapsed = end - start;
            remaining = timeout - elapsed;
        }while (remaining > 0);
        return elements;
    }

    @Override
    public boolean batchOffer(Collection<E> elements) {
        if(elements == null || elements.isEmpty()){
            return false;
        }
        boolean successful = false;
        try {
            for (E e : elements) {
                boolean offered = blockingQueue.offer(e, 1, TimeUnit.MILLISECONDS);
                if(offered){
                    successful = true;
                }
            }
        }catch (InterruptedException e){
            //ignore.
        }
        return successful;
    }

    public int size(){
        return blockingQueue.size();
    }

    public String toString(){
        return this.blockingQueue.toString();
    }
}
