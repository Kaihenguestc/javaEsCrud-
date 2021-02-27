import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class EmployeeSearch {
    
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        prepareData(client);
        executeSearch(client);
        client.close();
    }

    private static void executeSearch(TransportClient client) {
        SearchResponse response = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.matchQuery("position", "technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(0).to(40))
                .setFrom(0).setSize(1)
                .get();
        SearchHit[] searchHits = response.getHits().getHits();
        for (int i = 0; i < searchHits.length; i++) {
            System.out.println(searchHits[i].getSourceAsString());
        }

    }

    private static void prepareData(TransportClient client) throws IOException {
        client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("name", "kaiheng")
                        .field("age", 25)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2020-01-01")
                        .field("salary", 1000)
                    .endObject())
                .get();
        client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "zhangsan")
                        .field("age", 21)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2020-02-01")
                        .field("salary", 1000)
                        .endObject())
                .get();
        client.prepareIndex("company", "employee", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "lisi")
                        .field("age", 45)
                        .field("position", "technique software")
                        .field("country", "American")
                        .field("join_date", "2020-03-01")
                        .field("salary", 1000)
                        .endObject())
                .get();



    }
}
