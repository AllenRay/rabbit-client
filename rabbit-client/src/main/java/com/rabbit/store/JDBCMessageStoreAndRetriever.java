package com.rabbit.store;

import com.alibaba.fastjson.JSON;
import com.rabbit.props.OnePlusBasicProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.StringUtils;

import java.sql.*;
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

    private final Logger logger = LoggerFactory.getLogger(JDBCMessageStoreAndRetriever.class);

    private final static String QUERY_UN_SEND_MESSAGE_SQL = "SELECT * FROM message_store m INNER JOIN (SELECT message_id FROM message_store where " +
            "ROUND(UNIX_TIMESTAMP(now(4))*1000) - create_time > ? ORDER BY create_time desc LIMIT 0, 1000) m2 USING (message_id)";

    private final static String STORE_MESSAGE_SQL = "insert into message_store (message_key,message_exchange_key,message_routing_key,message_properties,message_payload,create_time)" +
            " values (?,?,?,?,?,ROUND(UNIX_TIMESTAMP(now(4))*1000))";

    private final static String REMOVE_MESSAGE_SQL = "delete from message_store where message_key = ?";

    private final static String REMOVE_MESSAGE_BY_ID = "delete from message_store where message_id = ?";

    private final static String QUERY_TOTAL_MESSAGES = "SELECT count(1) FROM message_store m INNER JOIN (SELECT message_id FROM message_store where " +
            "ROUND(UNIX_TIMESTAMP(now(4))*1000) - create_time > ? ORDER BY create_time desc) m2 USING (message_id)";

    private long compensationTime = 60000;

    @Override
    public List<MessageStoreBean> retrieveUnSendMessages() {

        return query(QUERY_UN_SEND_MESSAGE_SQL,new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement) throws SQLException {
                preparedStatement.setLong(1,getCompensationTime());
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
                        int messageId = rs.getInt("message_id");

                        messageStoreBean.setExchange(exchange);
                        messageStoreBean.setRoutingKey(routingKey);
                        if (!StringUtils.isEmpty(properties)) {
                            OnePlusBasicProperties basicProperties = JSON.parseObject(properties, OnePlusBasicProperties.class);
                            messageStoreBean.setBasicProperties(basicProperties.convertToBasicProperties());
                        }
                        messageStoreBean.setMessageKey(messageKey);
                        messageStoreBean.setPayload(bytes);
                        messageStoreBean.setMessageId(messageId);
                        messageStoreBeans.add(messageStoreBean);
                    }
                    return messageStoreBeans;
                }
                return Collections.emptyList();
            }
        });
    }

    @Override
    public int retrieveMessageTotalCount() {
        return query(QUERY_TOTAL_MESSAGES, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement) throws SQLException {
                preparedStatement.setLong(1,getCompensationTime());
            }
        }, new ResultSetExtractor<Integer>() {

            @Override
            public Integer extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                int count = 0;
                if (resultSet != null) {
                    while (resultSet.next()) {
                        count = resultSet.getInt(1);
                        break;
                    }
                }
                return count;
            }
        });
    }

    @Override
    public long getCompensationTime() {
        return this.compensationTime;
    }

    public void setCompensationTime(long compensationTime) {
        this.compensationTime = compensationTime;
    }

    @Override
    public Integer storeMessage(final MessageStoreBean messageStoreBean) {
        long start = System.currentTimeMillis();

        KeyHolder holder = new GeneratedKeyHolder();

        update(new PreparedStatementCreator(){

            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement preparedStatement = con.prepareStatement(STORE_MESSAGE_SQL,Statement.RETURN_GENERATED_KEYS);
                preparedStatement.setString(1, messageStoreBean.getMessageKey());
                preparedStatement.setString(2, messageStoreBean.getExchange());
                preparedStatement.setString(3, messageStoreBean.getRoutingKey());
                preparedStatement.setString(4, JSON.toJSONString(messageStoreBean.getBasicProperties()));
                preparedStatement.setBytes(5, messageStoreBean.getPayload());
                return preparedStatement;
            }
        },holder);

        long end = System.currentTimeMillis();

        logger.info("#MybatisLog# store message {} spent {}ms",holder.getKey().intValue(),end-start);

        return holder.getKey().intValue();
    }

    @Override
    public void removeMessage(final String messageKey) {
        long start = System.currentTimeMillis();

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

        long end = System.currentTimeMillis();

        logger.info("remove message {} spent {}ms",messageKey,end-start);
    }

    @Override
    public void removeMessageById(final int messageId) {
        long start = System.currentTimeMillis();

        execute(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                return connection.prepareStatement(REMOVE_MESSAGE_BY_ID);
            }
        }, new PreparedStatementCallback<Boolean>() {
            @Override
            public Boolean doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                preparedStatement.setInt(1, messageId);
                int count = preparedStatement.executeUpdate();
                return count > 1;
            }
        });

        long end = System.currentTimeMillis();

        logger.info("remove message {} spent {}ms",messageId,end-start);
    }

    @Override
    public void batchRemoveMessages(List<String> messageKeys) {
        long start = System.currentTimeMillis();

        BatchSqlUpdate batchSqlUpdate = new BatchSqlUpdate(getDataSource(),REMOVE_MESSAGE_SQL);
        batchSqlUpdate.setBatchSize(messageKeys.size());
        batchSqlUpdate.setTypes(new int[]{Types.VARCHAR});
        for(String message : messageKeys){
            batchSqlUpdate.update(new Object[]{message});
        }

        batchSqlUpdate.flush();

        long end = System.currentTimeMillis();
        logger.debug("Batch remove messages spent {}ms",end-start);
    }

    @Override
    public void batchRemoveMessageIds(List<Integer> messageIds) {
        long start = System.currentTimeMillis();

        BatchSqlUpdate batchSqlUpdate = new BatchSqlUpdate(getDataSource(),REMOVE_MESSAGE_BY_ID);
        batchSqlUpdate.setBatchSize(messageIds.size());
        batchSqlUpdate.setTypes(new int[]{Types.VARCHAR});
        for(int message : messageIds){
            batchSqlUpdate.update(new Object[]{message});
        }

        batchSqlUpdate.flush();

        long end = System.currentTimeMillis();
        logger.debug("Batch remove messages by id spent {}ms",end-start);
    }
}
