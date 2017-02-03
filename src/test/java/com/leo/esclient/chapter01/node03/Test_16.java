package com.leo.esclient.chapter01.node03;

import com.leo.esclient.util.FormatUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * 取回多个文档
 * Elasticsearch 的速度已经很快了，但甚至能更快。 将多个请求合并成一个，避免单独处理每个请求花费的网络时延和开销。
 * 如果你需要从 Elasticsearch 检索很多文档，那么使用 multi-get 或者 mget API 来将这些检索请求放在一个请求中，
 * 将比逐个文档请求更快地检索到全部文档。
 * <p/>
 * mget API 要求有一个 docs 数组作为参数，每个 元素包含需要检索文档的元数据， 包括 _index 、 _type 和 _id 。
 * Created by liweibin on 2017/2/3 0003.
 */
public class Test_16 {

    /**
     * 如果你想检索一个或者多个特定的字段，那么你可以通过 _source 参数来指定这些字段的名字
     *
     * @throws IOException
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "   \"docs\" : [\n" +
                    "      {\n" +
                    "         \"_index\" : \"website\",\n" +
                    "         \"_type\" :  \"blog\",\n" +
                    "         \"_id\" :    2\n" +
                    "      },\n" +
                    "      {\n" +
                    "         \"_index\" : \"website\",\n" +
                    "         \"_type\" :  \"pageviews\",\n" +
                    "         \"_id\" :    1,\n" +
                    "         \"_source\": \"views\"\n" +
                    "      }\n" +
                    "   ]\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/_mget", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 如果想检索的数据都在相同的 _index 中（甚至相同的 _type 中），
     * 则可以在 URL 中指定默认的 /_index 或者默认的 /_index/_type 。
     * 你仍然可以通过单独请求覆盖这些值
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "   \"docs\" : [\n" +
                    "      { \"_id\" : 2 },\n" +
                    "      { \"_type\" : \"pageviews\", \"_id\" :   1 }\n" +
                    "   ]\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/_mget", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 如果所有文档的 _index 和 _type 都是相同的，你可以只传一个 ids 数组，而不是整个 docs 数组
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "   \"ids\" : [ \"2\", \"1\" ]\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/_mget", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

}
