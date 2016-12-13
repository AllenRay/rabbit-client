##rabbit client
 1. 基于rabbit client 3.5.6封装client，虽然依赖于spring的管理，但是区分Spring amqp,相对于Spring amqp更轻量级.
 2. 利用lyra来做Connection/channel/consumer recovery和retry
 3. 使用spring retry 来做 Producer和Consumer的retry,retry的逻辑目前很简单
 4. 提供消息producer和 consumer handler，来做发送和处理消息业务逻辑
 5. 实现消息补偿，实现分布式环境下，最终一致

##如何使用

1. 定义Connection Factory,连接字符串。如果是多个以逗号隔开,如果端口号不是默认的5672，那么格式就是host:post,host:port,host:port,ip和post 用冒号隔开,如果下面userName,password,virtualHost三个参数不传入，将不会使用默认值


        <bean id="channelFactory" class="com.rabbit.ChannelFactory" scope="singleton">
        <property name="addresses" value="xxx,xxxx,xxxx"/>
        <property name="userName" value="admin"/>
        <property name="password" value="123ABC"/>
        <property name="virtualHost" value="/email"/>
        <property name="cacheSize" value="20"/>
        <property name="connectionSize" value="2"/> //producer connection sizes
		</bean>

2. 定义 Rabbit template,在定义模板类的时候，可以将此模板绑定queue/exchange/routing key. 这样在发送和消息消费的时候不需要传入queue/exchange/routing key
   在定义rabbitTemplate的时候，可以决定是否开启消息补偿的功能，以及是否利用redis或者mysql做事务补偿的中间件
   使用redis做事务补偿的中间件的时候，zkConnect和messageStoreAndRetriever可以不用注入

        <bean id="rabbitTemplate" class="com.rabbit.RabbitTemplate">
          <property name="factory" ref="channelFactory"/>
		  <property name="messageConverter" ref="messageConverter"/> //定义消息的converter，默认有hessian和fastjson，根据需要自己初始化MessageConverter，然后注入
		  <property name="appName" value="系统名称"/> //必填，可以用来，识别这个queue上面的consumer来自哪个系统
		  <property name="needCompensation" value="true"/> //是否需要开启消息补偿
		  <property name="zkConnect" value="xxx:2181,xxx:2181"/> //zk 集群，消息补偿的线程必须获得zk的分布式锁,避免在分布式环境下，多个节点进行同时补偿
		  <property name="messageStoreAndRetriever" ref="messageStoreAndRetriever"/>//消息存储和查询的组件
		  <property name="jedisPool" ref="writeJedisPool"/> //配置redis pool
          <property name="shardedJedisPool" ref="readShardedJedisPool"/>

        </bean>

        //定义消息清理线程，如果使用redis作为事务补偿中间件，可以不用初始化此组件
        <bean id="messageCleanr" class="com.oneplus.common.rabbit.store.MessageCleanr">
            <property name="messageStore" ref="messageStoreAndRetriever"/>
            <property name="jedisPool" ref="writeJedisPool"/>
            <property name="appName" value="test"/>
        </bean>

        <bean id="messageStoreAndRetriever" class="com.oneplus.common.rabbit.store.JDBCMessageStoreAndRetriever">
            <property name="dataSource" ref="dataSource"/> //数据源
            <property name="compensationTime" value="30000"/> //入库多少毫秒之后的消息需要补偿
        </bean>

3. 定义queue,如果需要queue的自动创建，env必须不能为空并且值不能为prod，因为在生产环境不推荐使用自动创建queue。
        <bean class="com.rabbit.QueueDeclare" id="queueDeclare1">
           <property name="name" value="test2-1"/>
           <property name="channelFactory" ref="channelFactory"/>
	       <property name="env" value="test/dev"/>
        </bean>

        <bean class="com.rabbit.QueueDeclare" id="queueDeclare2">
            <property name="name" value="test2-2"/>
            <property name="channelFactory" ref="channelFactory"/>
		    <property name="env" value="test/dev"/>
        </bean>

4. 定义Exchange,并且进行绑定

        <bean class="com.rabbit.ExchangeDeclare" id="exchangeDeclare">
        <property name="channelFactory" ref="channelFactory"/>
        <property name="name" value="test-1"/>
        <property name="type" value="fanout"/>
        <property name="queueBinds">
        <list>
        <bean class="com.rabbit.QueueBind">
        <property name="queue" value="test2-1"/>
        </bean>
        <bean class="com.rabbit.QueueBind">
        <property name="queue" value="test2-2"/>
        </bean>
        </list>
        </property>
        </bean>


5. 定义producer handler，需要confirm的消息发送ack或者nack 后会调用此方法。根据exchange 和 routing key 来进行绑定，routing key 支 *,# 匹配. * 表示一个单词，# 表示多个单词，用“.”分开

        @rabbit.ProducerHandler(exchange = "${exchange}",routingKey="${routingKey}")
        public class TestProducerHandler extends ProducerHandler {
        @Override
        public void handle(Message message) {
        System.out.println("message send successful: "+message.getDeliveryTag());
        }
        }
6. 定义Consumerhandler，消息received后，并且转换为Message对象后，将会回调此方法，Message的messagebody是消息的业务对象。返回true表示消息消费成功，返回false表示nack。
   消息消费失败后，将不会重新回到队列，所以业务端要么自己定义DLQ，要么自己去做重试

        @rabbit.ConsumerHandler(queue="${queue}")
        public class TestConsumerHandler extends ConsumerHandler {
            @Override
            public boolean handle(Message message) {
              System.out.println("message received: "+message.getDeliveryTag());
              return true;
            }
        }

##发送消息代码示例，一般来说我推荐是在事务回调中去发送消息，使用spring TransactionSynchronization 注册一个事务钩子，在事务的afterCommit的发送消息，这样能够保证业务是肯定成功了的。

        final RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate"); //获取模板类
        Email email = new Email();
        email.setEmailContent("test");
        email.setEmailTempate("11111");
        Message message = new Message();
        message.setMessageBody(email);

第一个参数是消息对象，第二个exchange，第三个是routing key。
template 提供了很多模版方法，可供使用

        rabbitTemplate.sendMessage(message, "mail", "mail"}); //发送oneway message，消费发送后，将不会等待broker的ACK
		rabbitTemplate.sendConfirmMessage(message,"mail","mail"); //此消息需要等待broker的ACK，将会回调ProducerHandler，如果有
		//发送补偿消息，在发送消息之前，需要调用rabbitTemplae.storeMessage()方法，将消息持久化到DB中，存储消息一定要保证成功，比如和业务的JDBC的事务一起提交或者回滚
		//使用mysql进行消息补偿
		int messageId = rabbitTemplate.storeMessage(message,exchang,routingKey,basicProperties);
		rabbitTemplate.sendCompensationMessage(message,"mail","mail",messageId,basicProperties);//如果消息发送失败，消息补偿机制，将会从DB中把消息load出来，然后进行补偿

		//使用redis
		rabbitTemplate.sendCompensationMessage(message,"mail","mail",basicProperties);

##消息消费代码示例

        final RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate");
          rabbitTemplate.receive("mail-queue",100）//第二个参数，fetchSize，告诉broker 每次推多少消息
        });


