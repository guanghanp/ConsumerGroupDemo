package ConsumerGroupTest;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class ProducerThread implements Runnable{

    private final KafkaProducer<String,String> Producer;
    private final String topic;

    public ProducerThread(String brokers,String topic){
        Properties properties = getProperty(brokers);
        this.topic = topic;
        this.Producer = new KafkaProducer<String, String>(properties);
    }

    private static Properties getProperty(String brokers){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public void run() {
        System.out.println("start sending message");
        int i = 0;
        while (i<50){
            String sendMsg = "Producer message number:"+String.valueOf(++i);
            Producer.send(new ProducerRecord<String, String>(topic,i+"",sendMsg),new Callback(){

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        e.printStackTrace();
                    }
                    System.out.println("Producer Message: Partition:"+recordMetadata.partition()+",Offset:"+recordMetadata.offset());
                }
            });

            // thread sleep 2 seconds every time
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("finished sending message to kafka");
    }

}
