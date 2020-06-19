package ConsumerGroupTest;

public class GroupDemo {
    public static void main(String[] args){
        String brokers = "localhost:9092";
        String groupId = "group1";
        String topic = "three-partitions";
        int consumerNumber = 3;

        Thread producerThread = new Thread(new ProducerThread(brokers,topic));
        producerThread.start();

        ConsumerGroup consumerGroup = new ConsumerGroup(brokers,groupId,topic,consumerNumber);
        consumerGroup.start();
    }
}