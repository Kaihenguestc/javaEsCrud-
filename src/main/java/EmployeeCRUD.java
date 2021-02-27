import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;

public class EmployeeCRUD {

    @SuppressWarnings({"unchecked", "resource"})
    public static void main(String[] args) throws Exception {
        //构建client
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        //createEmployee(client);
        //getEmployee(client);
        //updateEmployee(client);
        deleteEmployee(client);
        client.close();
    }

    /**
     * 创建员工信息
     * @param client
     */
    private static void createEmployee(TransportClient client) throws IOException {
        IndexResponse response = client.prepareIndex("company1", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "likaiheng")
                        .field("age", 27)
                        .field("postion", "technique")
                        .field("country", "china")
                        .field("join_date", "2020-01-01")
                        .field("salary", 1000)
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 获取员工信息
     * @param client
     */
    private static void getEmployee(TransportClient client) throws Exception {
        GetResponse response = client.prepareGet("company", "employee", "1").get();
        System.out.println(response.getSourceAsString());
    }

    private static void updateEmployee(TransportClient client) throws Exception{
        UpdateResponse response = client.prepareUpdate("company", "employee", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("postion", "technique manager")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    private static void deleteEmployee(TransportClient client) throws Exception {
        DeleteResponse response = client.prepareDelete("company", "employee", "1").get();
        System.out.println(response.getResult());
    }
}
