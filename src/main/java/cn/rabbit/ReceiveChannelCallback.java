package cn.rabbit;


/**
 * Created by Allen lei on 2015/12/7.
 */
public interface ReceiveChannelCallback {

    boolean handleMessage(Object t);
}
