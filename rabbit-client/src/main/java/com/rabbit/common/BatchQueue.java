package com.rabbit.common;

import java.util.Collection;
import java.util.List;

/**
 * Created by leizhengyu on 11/29/16.
 *
 */
public interface BatchQueue<E> {

    List<E> batch(int batchSize) throws InterruptedException;

    List<E> batch(int batchSize, long timeout)throws InterruptedException;

    boolean batchOffer(Collection<E> elements);
}
