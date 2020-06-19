package ConsumerGroupTest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerThread implements Runnable{

    private KafkaConsumer<String,String> kafkaConsumer;
    private final String topic;

    public ConsumerThread(String brokers,String groupId,String topic){
        Properties properties = getProperty(brokers,groupId);
        this.topic = topic;
        this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
        this.kafkaConsumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties getProperty(String brokers,String groupId){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "10000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }


    @Override
    public void run() {
        boolean run = true;
        while (run){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> item : consumerRecords){
                System.out.println("Consumer Message - Thread:"+Thread.currentThread().getName()+", value: "+item.value()+", Partition:"+item.partition()+", Offset:"+item.offset());
                if(item.key()!=null && item.key().equals("20")){
                    System.out.println(Thread.currentThread().getName()+" is closed");
                    kafkaConsumer.close();
                    run = false;
                    break;
                }
            }
        }
    }
}
