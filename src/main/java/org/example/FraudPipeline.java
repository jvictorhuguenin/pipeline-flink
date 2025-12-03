package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

public class FraudPipeline {

    public static void main(String[] args) {

        FlinkPipelineOptions options =
                PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setFlinkMaster("[local]");
        options.setParallelism(1);
        options.setCheckpointingInterval(10000L); // 10 segundos
        options.setMaxBundleTimeMills(1000L); // flush rápido
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        p.apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("input-topic")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
//                        .withConsumerConfigUpdates(
//                                ImmutableMap.of(
//                                        "auto.offset.reset", "earliest",
//                                        "enable.auto.commit", "false",
//                                        "group.id", "beam-test-group",
//                                        "client.id", "beam-debug-client"
//                                )
//                        )
                        .withoutMetadata()
                )
                .apply(Values.<String>create())
                .apply("CallRestAPI", ParDo.of(new RestPredictFn()))
                .apply("WriteToMySQL", ParDo.of(new MySQLSinkFn()));

        p.run().waitUntilFinish();
    }
}
