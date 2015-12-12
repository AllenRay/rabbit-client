基于rabbit client 3.5.6封装client，虽然依赖于spring的管理，但是区分Spring amqp

相对于Spring amqp更轻量级。大多数特性全部是rabbit client提供的，比如auto recovery。

使用也比较简单。

<context:annotation-config></context:annotation-config>



<bean id="channelFactory" class="cn.rabbit.ChannelFactory" scope="singleton"> //定义Connection Factory，
 <property name="addresses" value="172.21.xxx.206,172.21.xxx.128,172.21.xxx.129"></property>//连接字符串。如果是多个以逗号隔开,如果端口号不是默认的5672，那么格式就是172.21.107.206:xxxx,172.21.107.128:xxxx,172.21.107.129,ip和post 用冒号隔开
  //如果下面三个参数不传入，默认将会使用guest/guest并且virtual host是 “/”,在生产环境下，guest 用户将会被删除。
 <property name="userName" value="admin"/> //用户名
 <property name="password" value="123ABC"/> //密码
 <property name="virtualHost" value="/email"/> //virtualhost
</bean>

<bean id="rabbitTemplate" class="cn.rabbit.RabbitTemplate"> //定义 Rabbit template,在定义模板类的时候，可以将此模板绑定queue/exchange/routing key. 这样在发送和消息消费的时候不需要传入queue/exchange/routing key
   <property name="factory" ref="channelFactory"/>
</bean>

<bean class="cn.rabbit.QueueDeclare" id="queueDeclare1"> //定义queue，注意queue的定义必须放在Exchange前面，否则在绑定的时候会出错
    <property name="name" value="oneplus-test2-1"/>
    <property name="channelFactory" ref="channelFactory"/>
</bean>

<bean class="cn.rabbit.QueueDeclare" id="queueDeclare2">
    <property name="name" value="oneplus-test2-2"/>
    <property name="channelFactory" ref="channelFactory"/>
</bean>


<bean class="cn.rabbit.ExchangeDeclare" id="exchangeDeclare">// 定义Exchange
        <property name="channelFactory" ref="channelFactory"/>
    <property name="name" value="test-1"/>
    <property name="type" value="fanout"/> //fanout 类型，可以定义direct，topic
    <property name="queueBinds">
        <list>
          <bean class="com.oneplus.common.rabbit.QueueBind"> //进行queue和Exchange的绑定
              <property name="queue" value="oneplus-test2-1"/>
          </bean>
          <bean class="com.oneplus.common.rabbit.QueueBind">
              <property name="queue" value="oneplus-test2-2"/>
          </bean>
        </list>
    </property>
</bean>

--定义producer handler，消息发送ack或者nack 后会调用此方法。根据exchange 和 routing key 来进行绑定，routing key 支 *，# 匹配
--* 表示一个单词，# 表示多个单词，用“.”分开
@cn.rabbit.ProducerHandler(exchange = "${exchange}",routingKey="${routingKey}")
public class TestProducerHandler extends ProducerHandler {

    @Override
    public void handle(Message message) {
        System.out.println("message send successful: "+message.getDeliveryTag());
    }

}
--定义Consumerhandler，消息received后，并且转换为Message对象后，将会回调此方法，Message的messagebody是消息的业务对象。
--返回true表示消息消费成功，返回false表示nack。并且将会requeue。
@cn.rabbit.ConsumerHandler(queue="${queue}")
public class TestConsumerHandler extends ConsumerHandler {

    @Override
    public boolean handle(Message message) {
        System.out.println("message received: "+message.getDeliveryTag());
        return true;
    }
}

--发送消息代码示例
final RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate"); //获取模板类

 Email email = new Email();
 email.setEmailContent("test");
 email.setEmailTempate("11111");
 Message message = new Message();
 message.setMessageBody(email);

--第一个参数是消息对象，第二个exchange，第三个是routing key。
--template 提供了很多模版方法，可供使用
rabbitTemplate.sendMessage(message, "mail", "mail"});

--消息消息代码示例
final RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate");

rabbitTemplate.receive("mail-queue"）
});