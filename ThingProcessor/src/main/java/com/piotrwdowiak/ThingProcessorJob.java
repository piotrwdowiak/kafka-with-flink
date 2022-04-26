package com.piotrwdowiak;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;

public class ThingProcessorJob
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> kafkaSource = createKafkaSimpleStringSource("localhost:9092", "things_to_flink");

		KafkaSink<String> kafkaSink = createKafkaSimpleStringSink("localhost:9092", "things_from_flink");

		DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
		stream
			.map(i -> i.toUpperCase(Locale.ROOT))
			.sinkTo(kafkaSink);

		env.execute();
	}

	private static KafkaSource<String> createKafkaSimpleStringSource(String broker, String topic) {
		return KafkaSource.<String>builder()
				.setBootstrapServers(broker)
				.setTopics(topic)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}

	private static KafkaSink<String> createKafkaSimpleStringSink(String broker, String topic) {
		return KafkaSink.<String>builder()
				.setBootstrapServers(broker)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(topic)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
	}
}
