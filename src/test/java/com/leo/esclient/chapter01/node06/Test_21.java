package com.leo.esclient.chapter01.node06;

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
 * 映射
 * Created by liweibin on 2017/2/4 0004.
 */
public class Test_21 {

    /**
     * 查看映射
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/gb");
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 删除索引
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "DELETE",
                    "/gb");
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 创建索引
     * 若映射已经存在，可以增加，但不可修改
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"mappings\": {\n" +
                    "    \"tweet\" : {\n" +
                    "      \"properties\" : {\n" +
                    "        \"tweet\" : {\n" +
                    "          \"type\" :    \"string\",\n" +
                    "          \"analyzer\": \"english\"\n" +
                    "        },\n" +
                    "        \"date\" : {\n" +
                    "          \"type\" :   \"date\"\n" +
                    "        },\n" +
                    "        \"name\" : {\n" +
                    "          \"type\" :   \"string\"\n" +
                    "        },\n" +
                    "        \"user_id\" : {\n" +
                    "          \"type\" :   \"long\"\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/gb",
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
     * 在 tweet 映射增加一个新的名为 tag 的 not_analyzed 的文本域，使用 _mapping
     */
    @Test
    public void test_04() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"properties\" : {\n" +
                    "    \"tag\" : {\n" +
                    "      \"type\" :    \"string\",\n" +
                    "      \"index\":    \"not_analyzed\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/gb/_mapping/tweet",
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
     * 使用 analyze API 测试字符串域的映射
     */
    @Test
    public void test_05() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"field\": \"tweet\",\n" +
                    "  \"text\": \"Black-cats\" \n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/gb/_analyze",
                    Collections.<String, String>emptyMap(),
                    entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));

            query = "{\n" +
                    "  \"field\": \"tag\",\n" +
                    "  \"text\": \"Black-cats\" \n" +
                    "}";

            entity = new NStringEntity(query);
            indexResponse = restClient.performRequest(
                    "GET",
                    "/gb/_analyze",
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
