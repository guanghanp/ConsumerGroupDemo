package ConsumerGroupTest;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup{

    private List<ConsumerThread> consumerThreadList = new ArrayList<ConsumerThread>();

    public ConsumerGroup(String brokers,String groupId,String topic, int consumerNumber){
        for(int i = 0; i< consumerNumber;i++){
            ConsumerThread consumerThread = new ConsumerThread(brokers,groupId,topic);
            consumerThreadList.add(consumerThread);
        }
    }


    public void start(){

        for (ConsumerThread item : consumerThreadList){
            Thread thread = new Thread(item);
            thread.start();
        }
        
    }
}
