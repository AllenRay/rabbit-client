package cn.rabbit;

import cn.rabbit.exception.RabbitChannelException;
import cn.rabbit.exception.RabbitConnectionException;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * Created by allen lei on 2015/12/4.
 * 使用一个connection 管理多个channel。
 * 准备对channel 进行缓存，但是现阶段还没必要。
 *
 */
public class ChannelFactory {

     private final Logger logger = LoggerFactory.getLogger(ChannelFactory.class);

     private String address;

     private String host;

     private int port;

     private String userName;

     private String password;

     private String virtualHost;

     private boolean autoRecovery = true;

     private ConnectionFactory factory;

     private Connection connection;

     private ExecutorService executorService;


    /**
     * 初始化connection，如果传入了connectionfactory，那么将使用传入的connectionFactory，
     * 如果没有就构造一个默认的connectionFactory，然后根据注入的设置一些属性。
     */
     @PostConstruct
     public void initConnection(){

       try {
           if(getFactory() == null){
              factory = new ConnectionFactory();
           }

           if(isAutoRecovery()){
               factory.setAutomaticRecoveryEnabled(isAutoRecovery());
           }

           if(!StringUtils.isEmpty(getUserName())){
               factory.setUsername(getUserName());
           }
           if(!StringUtils.isEmpty(getPassword())){
               factory.setPassword(getPassword());
           }
           if(!StringUtils.isEmpty(getVirtualHost())){
               factory.setVirtualHost(getVirtualHost());
           }

           if(!StringUtils.isEmpty(getAddress())){
               String [] adds = getAddress().split(",");
               if(adds == null || adds.length == 0){
                   throw new RabbitConnectionException("Connect address is empty.");
               }
               Address [] addresses = new Address[adds.length];
               for(int i = 0; i < adds.length; i++){
                   String connectAdd = adds[0];
                   if(connectAdd.indexOf(":") > 0){
                       String[] hostAndPort = connectAdd.split(":");
                       addresses[i] = new Address(hostAndPort[0],Integer.valueOf(hostAndPort[1]));
                   }else {
                       addresses[i] = new Address(adds[0]);
                   }
               }

               if(getExecutorService() != null){
                  connection = factory.newConnection(getExecutorService(),addresses);
               }else {
                  connection = factory.newConnection(addresses);
               }
           }else {
               if(!StringUtils.isEmpty(getHost())) {
                   factory.setHost(getHost());
               }
               if(!StringUtils.isEmpty(getPort())) {
                   factory.setPort(getPort());
               }

               if(getExecutorService() != null){
                   connection = factory.newConnection(getExecutorService());
               }else {
                   connection = factory.newConnection();
               }
           }

       } catch (IOException e) {
           logger.error("Init rabbit connection occur error {}",e);
           throw new RabbitConnectionException("Init rabbit connection occur io error,the error is: "+e);
       } catch (TimeoutException e) {
           logger.error("Init rabbit connection occur error {}",e);
           throw new RabbitConnectionException("Init rabbit connection occur timeout error,the error is: "+e);
       }

       if(connection == null){
          logger.error("Create connection occur error.");
          throw new RabbitConnectionException("Rabbit connection has not instance,so abort it...");
       }

     }

    /**
     * 获取新的channel
     * @return
     *
     * TODO 将会把channel 进行缓存。
     */
     public Channel getChannel(){
         if(connection == null){
             logger.error("No available rabbit connection");
             throw new RabbitConnectionException("No available rabbit connection.");
         }

         return createNewChannel();

     }

    private Channel createNewChannel() {
        try {
            if(logger.isDebugEnabled()){
                logger.debug("create a new rabbit channel....");
            }
            Channel channel = connection.createChannel();
            return  channel;
        } catch (IOException e) {
            logger.error("Create channel failed,the reason is: {}",e);
            throw new RabbitChannelException("Create channel failed,the reason is: "+e);
        }
    }

    /**
     * 关闭channel
     * @param channel
     *  TODO 如果将channel 进行缓存，将不会直接关闭这个连接
     */
    public void closeChannel(Channel channel){
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                logger.error("close channel occur io error,the error is : {}",e);
                throw new RabbitChannelException("close channel occur io error,the error is : "+e);
            } catch (TimeoutException e) {
                logger.error("close channel time out,the error is : {}",e);
                throw new RabbitChannelException("close channel time out,the error is : "+e);
            }
        }
    }

    /**
     * 不需要显示的关闭所有channel，如果connection关闭了，将会自动关闭这个connection上面的所有channel
     */
    @PreDestroy
    public void closeConnection(){
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("Close rabbit connection failed,the error is: {}",e);
                throw  new RabbitConnectionException("Close rabbit connection failed,the error is: "+e);
            }
        }


    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAutoRecovery() {
        return autoRecovery;
    }

    public void setAutoRecovery(boolean autoRecovery) {
        this.autoRecovery = autoRecovery;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public ConnectionFactory getFactory() {
        return factory;
    }

    public void setFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
