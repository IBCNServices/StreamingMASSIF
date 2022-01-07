package idlab.massif.sources;

import idlab.massif.core.PipeLine;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SourceInf;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerSource implements SourceInf {

    private final String kafkaServer;
    private final String kafkaTopic;
    private final Properties props;
    private PipeLine pipeline;
    private ListenerInf listener;

    public KafkaConsumerSource(String kafkaServer, String kafkaTopic) {
        this.kafkaServer = kafkaServer;
        this.kafkaTopic = kafkaTopic;
        this.props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("group.id", "MASSIF_"+System.currentTimeMillis());
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void main(String[] args){
        KafkaConsumerSource source = new KafkaConsumerSource("localhost:9092","quickstart-events");
        source.stream();




    }
    public void registerPipeline(PipeLine pipeline) {
        this.pipeline = pipeline;
    }
    public void stream(){
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList(this.kafkaTopic),new SeekToEnd(consumer));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if(!records.isEmpty()) {
                    System.out.println(System.currentTimeMillis()+ ": # kafka messages received: " + records.count());
                }
                for (ConsumerRecord<String, String> record : records)
                {

                    if (pipeline != null) {
                        this.pipeline.addEvent(record.value());
                    }
                    if (listener != null) {
                        this.listener.notify(0, record.value());
                    }

                }
            }
        } catch(Exception e){
            e.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }

    @Override
    public boolean addEvent(String event) {
        return false;
    }

    @Override
    public boolean addListener(ListenerInf listener) {
        this.listener = listener;
        return false;
    }

    @Override
    public void start() {
        Runnable task = () -> {
            this.stream();
        };
        Thread thread = new Thread(task);
        thread.start();
    }

    @Override
    public void stop() {

    }

    public static class SeekToEnd implements ConsumerRebalanceListener {
        private final KafkaConsumer consumer;

        public SeekToEnd(KafkaConsumer consumer) {
            this.consumer = consumer;
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            consumer.seekToEnd(partitions);

        }
    }
    @Override
    public String toString() {
        return String.format("{\"type\":\"Source\",\"impl\":\"KafkaConsumerSource\",\"option1\":\"%s\",\"option2\":\"%s\"}", kafkaServer,kafkaTopic);
    }
}
