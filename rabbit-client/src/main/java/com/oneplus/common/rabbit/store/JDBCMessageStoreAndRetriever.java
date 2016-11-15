package com.oneplus.common.rabbit.store;

import com.alibaba.fastjson.JSON;
import com.oneplus.common.rabbit.props.OnePlusBasicProperties;
import com.rabbitmq.client.AMQP;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by allen lei on 2016/8/8.
 * version 1.0.0
 * company oneplus
 * 提供默认的jdbc实现存储，删除以及查询消息实现
 */
public class JDBCMessageStoreAndRetriever extends JdbcTemplate implements MessageStoreAndRetriever {

    private final static String QUERY_UN_SEND_MESSAGE_SQL = "SELECT * FROM message_store m INNER JOIN (SELECT message_id FROM oneplus_message_store where ROUND(UNIX_TIMESTAMP(now(4))*1000) - create_time > 1000 ORDER BY create_time LIMIT 0, 100) m2 USING (message_id)";

    private final static String STORE_MESSAGE_SQL = "insert into message_store (message_key,message_exchange_key,message_routing_key,message_properties,message_payload,create_time)" +
            " values (?,?,?,?,?,?)";

    private final static String REMOVE_MESSAGE_SQL = "delete from message_store where message_key = ?";

    @Override
    public List<MessageStoreBean> retrieveUnSendMessages() {

        return query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                return con.prepareStatement(QUERY_UN_SEND_MESSAGE_SQL);
            }
        }, new ResultSetExtractor<List<MessageStoreBean>>() {
            @Override
            public List<MessageStoreBean> extractData(ResultSet rs) throws SQLException, DataAccessException {
                if (rs != null) {
                    List<MessageStoreBean> messageStoreBeans = new ArrayList<>();
                    while (rs.next()) {
                        MessageStoreBean messageStoreBean = new MessageStoreBean();
                        String exchange = rs.getString("message_exchange_key");
                        String routingKey = rs.getString("message_routing_key");
                        String properties = rs.getString("message_properties");
                        String messageKey = rs.getString("message_key");
                        byte[] bytes = rs.getBytes("message_payload");

                        messageStoreBean.setExchange(exchange);
                        messageStoreBean.setRoutingKey(routingKey);
                        if (!StringUtils.isEmpty(properties)) {
                            OnePlusBasicProperties basicProperties = JSON.parseObject(properties, OnePlusBasicProperties.class);
                            messageStoreBean.setBasicProperties(basicProperties.convertToBasicProperties());
                        }
                        messageStoreBean.setMessageKey(messageKey);
                        messageStoreBean.setPayload(bytes);
                        messageStoreBeans.add(messageStoreBean);
                    }
                    return messageStoreBeans;
                }
                return Collections.emptyList();
            }
        });
    }

    @Override
    public void storeMessage(final MessageStoreBean messageStoreBean) {
        execute(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(STORE_MESSAGE_SQL);
            }
        }, new PreparedStatementCallback<Boolean>() {

            @Override
            public Boolean doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                preparedStatement.setString(1, messageStoreBean.getMessageKey());
                preparedStatement.setString(2, messageStoreBean.getExchange());
                preparedStatement.setString(3, messageStoreBean.getRoutingKey());
                preparedStatement.setString(4, JSON.toJSONString(messageStoreBean.getBasicProperties()));
                preparedStatement.setBytes(5, messageStoreBean.getPayload());
                preparedStatement.setLong(6, System.currentTimeMillis());
                int count = preparedStatement.executeUpdate();
                return count > 0;
            }
        });
    }

    @Override
    public void removeMessage(final String messageKey) {
        execute(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(REMOVE_MESSAGE_SQL);
            }
        }, new PreparedStatementCallback<Boolean>() {
            @Override
            public Boolean doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                preparedStatement.setString(1, messageKey);
                int count = preparedStatement.executeUpdate();
                return count > 1;
            }
        });
    }
}
