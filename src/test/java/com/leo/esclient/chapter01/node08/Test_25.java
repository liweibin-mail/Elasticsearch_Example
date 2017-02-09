package com.leo.esclient.chapter01.node08;

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
 * 排序
 * Created by liweibin on 2017/2/8 0008.
 */
public class Test_25 {

    /**
     * 返回所有 user_id 字段包含1点结果
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"bool\" : {\n" +
                    "            \"filter\" : {\n" +
                    "                \"term\" : {\n" +
                    "                    \"user_id\" : 1\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/_search",
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
     * 按照字段值排序
     * 注：若 _score 不用于排序，将不被计算
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"bool\" : {\n" +
                    "            \"filter\" : { " +
                    "               \"range\" : { \"age\" : {\"gt\" : 30 }}" +
                    "              }\n" +
                    "        }\n" +
                    "    },\n" +
                    "    \"sort\": { \"age\": { \"order\": \"desc\" }}\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/megacorp/_search",
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
     * 多级排序
     * 首先按照年龄排序，然后按照相关性得分 排序
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"bool\" : {\n" +
                    "            \"must\":   { \"match\": { \"interests\": \"forestry\" }},\n" +
                    "            \"filter\" : { \"term\" : { \"age\" : 35 }}\n" +
                    "        }\n" +
                    "    },\n" +
                    "    \"sort\": [\n" +
                    "        { \"age\":   { \"order\": \"desc\" }},\n" +
                    "        { \"_score\": { \"order\": \"desc\" }}\n" +
                    "    ]\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/megacorp/_search",
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
     * 字段多值排序
     * 按照date字段中最早的日期排序
     */
    @Test
    public void test_04() {
        String query = "\"sort\": {\n" +
                "    \"dates\": {\n" +
                "        \"order\": \"asc\",\n" +
                "        \"mode\":  \"min\"\n" +
                "    }\n" +
                "}";
        System.out.println(query);
    }
}
