import json
from confluent_kafka import Producer

# Kafka服务器的地址
bootstrap_servers = 'hadoop1:9092'

# Kafka主题
topic = 'my_topic'

# Kafka生产者配置
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'producer'
}

# 创建Kafka生产者
producer = Producer(producer_config)

# 要发送的JSON数据
data = {
    'name': 'John',
    'age': 31,
    'city': 'New York'
}

# 将JSON数据转换为字符串
message_value = json.dumps(data)

# 发送消息到Kafka主题
producer.produce(topic, value=message_value)

# 等待所有消息发送完成
producer.flush()

# 关闭Kafka生产者
# producer.close()
