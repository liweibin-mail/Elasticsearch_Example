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
 * 代价较小的批量操作
 * 与 mget 可以使我们一次取回多个文档同样的方式， bulk API 允许在单个步骤中进行多次
 * create 、 index 、 update 或 delete 请求。 如果你需要索引一个数据流比如日志事件，
 * 它可以排队和索引数百或数千批次。
 * 这种格式类似一个有效的单行 JSON 文档 流 ，它通过换行符(\n)连接到一起。注意两个要点：
 * 1)每行一定要以换行符(\n)结尾， 包括最后一行 。这些换行符被用作一个标记，可以有效分隔行。
 * 2)这些行不能包含未转义的换行符，因为他们将会对解析造成干扰。这意味着这个 JSON 不 能使用 pretty 参数打印。
 * <p/>
 * @link http://106.186.120.253/preview/bulk.html#CO18-1
 * Created by liweibin on 2017/2/3 0003.
 */
public class Test_17 {

    /**
     *
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{ \"delete\": { \"_index\": \"website\", \"_type\": \"blog\", \"_id\": \"123\" }} \n" +
                    "{ \"create\": { \"_index\": \"website\", \"_type\": \"blog\", \"_id\": \"123\" }}\n" +
                    "{ \"title\":    \"My first blog post\" }\n" +
                    "{ \"index\":  { \"_index\": \"website\", \"_type\": \"blog\" }}\n" +
                    "{ \"title\":    \"My second blog post\" }\n" +
                    "{ \"update\": { \"_index\": \"website\", \"_type\": \"blog\", \"_id\": \"123\", \"_retry_on_conflict\" : 3} }\n" +
                    "{ \"doc\" : {\"title\" : \"My updated blog post\"} }\n";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "POST",
                    "/_bulk", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
