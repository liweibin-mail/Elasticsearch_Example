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
 * 乐观并发控制
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_14 {

    /**
     * 通过重建文档的索引来保存修改，我们指定 version 为我们的修改会被应用的版本
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"title\": \"My second blog entry\",\n" +
                    "  \"text\":  \"Starting to get the hang of this...\"\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/1?version=10", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 通过外部系统使用版本控制
     * 外部版本号的处理方式和我们之前讨论的内部版本号的处理方式有些不同，
     * Elasticsearch 不是检查当前 _version 和请求中指定的版本号是否相同，
     * 而是检查当前 _version 是否 小于 指定的版本号。 如果请求成功，
     * 外部的版本号作为文档的新 _version 进行存储。
     * <p/>
     * 外部版本号不仅在索引和删除请求是可以指定，而且在 创建 新文档时也可以指定。
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
                    "  \"text\":  \"Starting to get the hang of this...\"\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            //即使原文档version不是外部version，在PUT时也可以置为外部version
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/2?version=3&version_type=external", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));

            //现在我们更新这个文档，指定一个新的 version 号是 10 ：
            indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/2?version=10&version_type=external", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
