package org.example;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;


public class FraudPipeline {

    public static void main(String[] args) throws Exception {

        // -----------------------------------------------------
        // FLINK EXECUTION ENVIRONMENT
        // -----------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // -----------------------------------------------------
        // KAFKA SOURCE (API MODERNA DO FLINK)
        // -----------------------------------------------------
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-topic")
                .setGroupId("fraud-flink-native")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        // -----------------------------------------------------
        // PRINT RAW KAFKA MESSAGES
        // -----------------------------------------------------
        stream.map(msg -> {
            System.out.println("RECEBIDO DO KAFKA → " + msg);
            return msg;
        });

        DataStream<String> enriched = stream.map(new OnnxPredictMapFunction());

        // -----------------------------------------------------
        // FILTRAR APENAS FRAUDES (classe = 1)
        // -----------------------------------------------------
        DataStream<String> fraudOnly = enriched.filter(json -> json.startsWith("FRAUD:"));

        // -----------------------------------------------------
        // WRITE TO MYSQL
        // -----------------------------------------------------
        fraudOnly.addSink(new MySQLSink());

        // -----------------------------------------------------
        // EXECUTE
        // -----------------------------------------------------
        env.execute("Flink Native Fraud Detection Pipeline");
    }

    // =========================================================================
// REST CALL MAP FUNCTION (FLINK-COMPATIBLE)
// =========================================================================
    public static class RestPredictMapFunction extends RichMapFunction<String, String> {

        private transient HttpClient client;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) throws Exception {
            // HttpClient MUST be initialized here to avoid serialization problems
            this.client = HttpClient.newHttpClient();
            this.mapper = new ObjectMapper();
        }

        @Override
        public String map(String jsonRecord) throws Exception {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("http://localhost:8080/predict"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonRecord))
                    .build();

            // REST call
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            String body = response.body();

            boolean isFraud = body.contains("\"is_fraud\":true");

            if (isFraud) {
                return "FRAUD:" + jsonRecord;
            } else {
                return "OK:" + jsonRecord;
            }
        }
    }

    public static class OnnxPredictMapFunction extends RichMapFunction<String, String> {

        private transient OrtEnvironment env;
        private transient OrtSession session;

        @Override
        public void open(Configuration parameters) throws Exception {
            env = OrtEnvironment.getEnvironment();
            session = env.createSession("modelo.onnx");
        }

        @Override
        public String map(String jsonString) throws Exception {

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> row = mapper.readValue(jsonString, Map.class);

            Collection<Object> values = row.values();

            float[] features = new float[values.size()];
            int i = 0;

            for (Object v : values) {
                features[i++] = ((Number) v).floatValue();
            }

            OnnxTensor inputTensor = OnnxTensor.createTensor(env, new float[][]{features});

            OrtSession.Result result = session.run(
                    Collections.singletonMap("input", inputTensor)
            );

            long[] pred = (long[]) result.get(0).getValue();
            boolean isFraud = pred[0] == 1L;

            if (isFraud)
                return "FRAUD:" + jsonString;
            else
                return "OK:" + jsonString;
        }
    }


    // =========================================================================
    // MYSQL SINK
    // =========================================================================
    public static class MySQLSink implements org.apache.flink.streaming.api.functions.sink.SinkFunction<String> {

        private transient Connection conn;
        private transient PreparedStatement stmt;

        @Override
        public void invoke(String value, Context ctx) throws Exception {

            if (conn == null) {
                conn = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/pipeline",
                        "root",
                        "rootpass"
                );
                stmt = conn.prepareStatement("INSERT INTO classified_entries (payload) VALUES (?);");
            }

            String payload = value.replace("FRAUD:", "");

            stmt.setString(1, payload);
            stmt.executeUpdate();
        }
    }
}