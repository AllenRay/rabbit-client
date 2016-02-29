##rabbit client
 1. 基于rabbit client 3.5.6封装client，虽然依赖于spring的管理，但是区分Spring amqp,相对于Spring amqp更轻量级.
 2. 利用lyra来做Connection/channel/consumer recovery和retry
 3. 使用spring retry 来做 Producer和Consumer的retry,retry的逻辑目前很简单
 4. 提供消息producer和 consumer handler，来做发送和处理消息业务逻辑

##如何使用

1. 定义Connection Factory,连接字符串。如果是多个以逗号隔开,如果端口号不是默认的5672，那么格式就是host:post,host:port,host:port,ip和post 用冒号隔开,如果下面userName,password,virtualHost三个参数不传入，将不会使用默认值


        <bean id="channelFactory" class="rabbit.ChannelFactory" scope="singleton">
        <property name="addresses" value="xxx,xxxx,xxxx"/>
        <property name="userName" value="admin"/>
        <property name="password" value="123ABC"/>
        <property name="virtualHost" value="/email"/>
        <property name="cacheSize" value="20"/>
        </bean>

2. 定义 Rabbit template,在定义模板类的时候，可以将此模板绑定queue/exchange/routing key. 这样在发送和消息消费的时候不需要传入queue/exchange/routing key

        <bean id="rabbitTemplate" class="rabbit.RabbitTemplate">
        <property name="factory" ref="channelFactory"/>
        </bean>

3. 定义queue，注意queue的定义必须放在Exchange前面，否则在绑定的时候会出错
        <bean class="rabbit.QueueDeclare" id="queueDeclare1">
        <property name="name" value="test2-1"/>
        <property name="channelFactory" ref="channelFactory"/>
        </bean>

        <bean class="rabbit.QueueDeclare" id="queueDeclare2">
        <property name="name" value="test2-2"/>
        <property name="channelFactory" ref="channelFactory"/>
        </bean>

4. 定义Exchange,并且进行绑定
        <bean class="rabbit.ExchangeDeclare" id="exchangeDeclare">
        <property name="channelFactory" ref="channelFactory"/>
        <property name="name" value="test-1"/>
        <property name="type" value="fanout"/>
        <property name="queueBinds">
        <list>
        <bean class="rabbit.QueueBind">
        <property name="queue" value="test2-1"/>
        </bean>
        <bean class="rabbit.QueueBind">
        <property name="queue" value="test2-2"/>
        </bean>
        </list>
        </property>
        </bean>


5. 定义producer handler，消息发送ack或者nack 后会调用此方法。根据exchange 和 routing key 来进行绑定，routing key 支 *,# 匹配. * 表示一个单词，# 表示多个单词，用“.”分开

        @rabbit.ProducerHandler(exchange = "${exchange}",routingKey="${routingKey}")
        public class TestProducerHandler extends ProducerHandler {
        @Override
        public void handle(Message message) {
        System.out.println("message send successful: "+message.getDeliveryTag());
        }
        }
6. 定义Consumerhandler，消息received后，并且转换为Message对象后，将会回调此方法，Message的messagebody是消息的业务对象。返回true表示消息消费成功，返回false表示nack。并且将会requeue。

        @rabbit.ConsumerHandler(queue="${queue}")
        public class TestConsumerHandler extends ConsumerHandler {
        @Override
        public boolean handle(Message message) {
        System.out.println("message received: "+message.getDeliveryTag());
        return true;
        }
        }

##发送消息代码示例

        final RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate"); //获取模板类
        Email email = new Email();
        email.setEmailContent("test");
        email.setEmailTempate("11111");
        Message message = new Message();
        message.setMessageBody(email);

第一个参数是消息对象，第二个exchange，第三个是routing key。
template 提供了很多模版方法，可供使用

        rabbitTemplate.sendMessage(message, "mail", "mail"});

##消息消息代码示例

        final RabbitTemplate rabbitTemplate = (RabbitTemplate)context.getBean("rabbitTemplate");
        rabbitTemplate.receive("mail-queue"）
        });

