DROP TABLE IF EXISTS `message_store`;
CREATE TABLE `oneplus_message_store` (
  `message_id` int(20) NOT NULL AUTO_INCREMENT,
  `message_key` varchar(200) NOT NULL,
  `message_exchange_key` varchar(200) NOT NULL,
  `message_routing_key` varchar(200) NOT NULL,
  `message_properties` varchar(4000),
  `message_payload` blob NOT NULL,
  `create_time` bigint(30) not null,
   PRIMARY KEY (`message_id`),
   KEY `message_key_index` (`message_key`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;