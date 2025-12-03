package org.example;

import org.apache.beam.sdk.transforms.DoFn;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RestPredictFn extends DoFn<String, String> {

    private transient HttpClient client;

    @Setup
    public void setup() {
        client = HttpClient.newHttpClient();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String jsonRecord = ctx.element();   // JSON recebido do Kafka

        try {
            String body = jsonRecord;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("http://localhost:8000/predict"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            String json = response.body();
            System.out.println("API Response: " + json);

            // {"is_fraud": true}
            boolean isFraud = json.contains("\"is_fraud\": true");

            if (isFraud) {
                ctx.output(jsonRecord);
            }

        } catch (Exception e) {
            System.err.println("Erro consultando API REST: " + e.getMessage());
        }
    }
}