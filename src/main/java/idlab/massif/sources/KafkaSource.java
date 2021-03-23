package idlab.massif.sources;


import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;


import idlab.massif.core.PipeLine;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SourceInf;





public class KafkaSource implements SourceInf {
	
	




	private PipeLine pipeline;
	private String kafkaServer;
	private String kafkaTopic;
	private Properties props;
	private ListenerInf listener;
	private KafkaStreams streams;
	private long timeOutTime;
	private long startTime;
	
	public KafkaSource(String kafkaServer, String kafkaTopic) {
		this.kafkaServer = kafkaServer;
		this.kafkaTopic = kafkaTopic;
		this.props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamingMassif-KafkaSink");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //latest?
		this.timeOutTime=30000;
		
		
	}
	public void registerPipeline(PipeLine pipeline) {
		this.pipeline = pipeline;
	}
	public void stream() {
		this.startTime = System.currentTimeMillis();
		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> sensors = builder.stream(this.kafkaTopic, Consumed.with(Serdes.String(), Serdes.String()));
		sensors
		.filter((key, value) ->(System.currentTimeMillis()-startTime)>timeOutTime)
		.foreach((key, value) -> {
			if(pipeline!=null) {
				this.pipeline.addEvent(value);
				}
				if(listener!=null) {
					this.listener.notify(0, value);
				}
		});
		

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, this.props);
		this.streams = streams;

		streams.cleanUp();

		streams.start();
	}

	@Override
	public boolean addEvent(String event) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean addListener(ListenerInf listener) {
		this.listener = listener;
		return false;
	}


	@Override
	public void start() {
		// TODO Auto-generated method stub
		this.stream();
	}
	@Override
	public void stop() {
		streams.close();
	}
	@Override
	public String toString() {
		return String.format("{\"type\":\"Source\",\"impl\":\"kafkaSource\",\"kafkaServer\":\"%s\",\"kafkaTopic\":\"%s\"}", kafkaServer,kafkaTopic);
	}
}
