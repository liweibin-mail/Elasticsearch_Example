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
 * 更新整个文档
 * 在 Elasticsearch 中文档是 不可改变 的，不能修改它们。
 * 相反，如果想要更新现有的文档，需要 重建索引 或者进行替换。
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_11 {

    /**
     * 在响应体中，我们能看到 Elasticsearch 已经增加了 _version 字段值,
     * 并且created 标志设置成 false ，是因为相同的索引、类型和 ID 的文档已经存在。
     * 在内部，Elasticsearch 已将旧文档标记为已删除，并增加一个全新的文档。
     * 尽管你不能再对旧版本的文档进行访问，但它并不会立即消失。当继续索引更多的数据，
     * Elasticsearch 会在后台清理这些已删除文档。
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"title\": \"My first blog entry\",\n" +
                    "  \"text\":  \"I am starting to get the hang of this...\",\n" +
                    "  \"date\":  \"2014/01/02\"\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/123", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 将test_01中被覆盖后的数据查询出来
     *
     * @throws IOException
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            HttpEntity entity = new NStringEntity("");
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/123", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
