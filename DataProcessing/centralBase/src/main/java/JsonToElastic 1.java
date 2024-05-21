import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JsonToElastic {

    private static final int FLUSH_THRESHOLD = 30;
    private static final HashMap<String, Integer> jsonRecordSize = new HashMap<>();
    private static final HashMap<String, Integer> jsonVersion = new HashMap<>();
    private final HashMap<String, List<String>> jsonMessages = new HashMap<>();
    private Long timestamp;
    private final ElasticsearchClient esClient;

    public JsonToElastic() throws IOException {
        timestamp = System.currentTimeMillis();

        for (int i = 1; i <= 10; i++) {
            jsonVersion.put(String.valueOf(i), 1);
            jsonRecordSize.put(String.valueOf(i), 0);
        }

        // Elasticsearch client setup
        String serverUrl ="https://76a792e7afa04db69e111520b30c3718.us-central1.gcp.cloud.es.io:443";
        String apiKey = "d2tHRG1JOEJlUmxTaW5kd2Zra1c6Y1prbzZPS3NUWjZSZGcxQ1lNS2RMZw==";

        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl))
                .setDefaultHeaders(new BasicHeader[]{
                        new BasicHeader("Authorization", "ApiKey " + apiKey)
                })
                .build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        esClient = new ElasticsearchClient(transport);
    }

    public void write(WeatherMessage weatherStatus) throws IOException {
        List<String> stationMessages = jsonMessages.getOrDefault(weatherStatus.getStationId(), new ArrayList<>());
        stationMessages.add(weatherStatus.toString());
        jsonMessages.put(weatherStatus.getStationId().toString(), stationMessages);

        updateRecordSize(weatherStatus);
        flushIfNeeded(weatherStatus);
    }

    private void updateRecordSize(WeatherMessage weatherStatus) {
        jsonRecordSize.put(weatherStatus.getStationId().toString(), jsonRecordSize.get(weatherStatus.getStationId()) + 1);
    }

    private void flushIfNeeded(WeatherMessage weatherStatus) throws IOException {
        if (jsonRecordSize.get(weatherStatus.getStationId()) >= FLUSH_THRESHOLD) {
            System.out.println("Flushing the JSON messages to Elasticsearch");

            List<String> stationMessages = jsonMessages.get(weatherStatus.getStationId());
            if (stationMessages != null) {
                writeJSON(stationMessages);
                stationMessages.clear();
            }

            jsonRecordSize.put(weatherStatus.getStationId().toString(), 0);
            jsonVersion.put(weatherStatus.getStationId().toString(), jsonVersion.get(weatherStatus.getStationId()) + 1);
        }
    }

    private void writeJSON(List<String> jsonMessages) throws IOException {
        String json = String.join("\n", jsonMessages); // Ensure newline-separated JSON
        sendToElasticSearch(json, "station");
    }

    private void sendToElasticSearch(String jsonContent, String indexName) throws IOException {
        BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();

        BufferedReader br = new BufferedReader(new StringReader(jsonContent));
        String line;
        while ((line = br.readLine()) != null) {
            BinaryData data = BinaryData.of(line.getBytes(), ContentType.APPLICATION_JSON);
            bulkRequestBuilder.operations(op -> op
                    .index(idx -> idx
                            .index(indexName)
                            .document(data)
                    )
            );
        }
        BulkResponse response = esClient.bulk(bulkRequestBuilder.build());

        if (response.errors()) {
            System.err.println("Bulk indexing had errors");
            response.items().forEach(item -> {
                if (item.error() != null) {
                    System.err.println(item.error().reason());
                }
            });
        } else {
            System.out.println("Bulk indexing completed successfully");
        }
    }

    public void close() throws IOException {
        if (esClient != null) {
            esClient._transport().close();
        }
    }
}
