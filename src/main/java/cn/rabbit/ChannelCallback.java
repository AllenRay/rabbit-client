package cn.rabbit;

import com.rabbitmq.client.Channel;

/**
 * Created by Administrator on 2015/12/7.
 */
public interface ChannelCallback {

    void handleChannelCallback(Channel channel);
}
