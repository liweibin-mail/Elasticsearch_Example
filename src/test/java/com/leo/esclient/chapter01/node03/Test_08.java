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
 * 一个文档的 _index 、 _type 和 _id 唯一标识一个文档。
 * 我们可以提供自定义的 _id 值，或者让 index API 自动生成。
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_08 {

    /**
     * 自定义 _id 值
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
                    "  \"text\":  \"Just trying this out...\",\n" +
                    "  \"date\":  \"2014/01/01\"\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/123",
                    Collections.<String, String>emptyMap(),
                    entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 如果你的数据没有自然的 ID， Elasticsearch 可以帮我们自动生成 ID 。
     * 请求的结构调整为： 不再使用 PUT (“使用这个 URL 存储这个文档”)，
     * 而是使用 POST (“存储文档在这个 URL 命名空间下”)。
     * 自动生成的 ID 是 URL-safe、 基于 Base64 编码且长度为20个字符的 GUID 字符串。
     * 这些 GUID 字符串由可修改的 FlakeID 模式生成，这种模式允许多个节点并行生成唯一 ID ，
     * 且互相之间的冲突概率几乎为零。
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

            String query = "{\n" +
                    "  \"title\": \"My second blog entry\",\n" +
                    "  \"text\":  \"Still trying this out...\",\n" +
                    "  \"date\":  \"2014/01/01\"\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "POST",
                    "/website/blog/",
                    Collections.<String, String>emptyMap(),
                    entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
