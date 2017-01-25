package com.leo.esclient.chapter01.node01;

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
 * 分析:
 * Elasticsearch 有一个功能叫聚合（aggregations），
 * 允许我们基于数据生成一些精细的分析结果。
 * 聚合与 SQL 中的 GROUP BY 类似但更强大。
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_05 {

    /**
     * 挖掘出雇员中最受欢迎的兴趣爱好
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "  \"aggs\": {\n" +
                    "    \"all_interests\": {\n" +
                    "      \"terms\": { \"field\": \"interests\" }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(queryStr);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "megacorp/employee/_search",
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
     * 查询叫 Smith 的雇员中最受欢迎的兴趣爱好
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "  \"query\": {\n" +
                    "    \"match\": {\n" +
                    "      \"last_name\": \"smith\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"aggs\": {\n" +
                    "    \"all_interests\": {\n" +
                    "      \"terms\": {\n" +
                    "        \"field\": \"interests\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(queryStr);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "megacorp/employee/_search",
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
     * 查询特定兴趣爱好员工的平均年龄
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "    \"aggs\" : {\n" +
                    "        \"all_interests\" : {\n" +
                    "            \"terms\" : { \"field\" : \"interests\" },\n" +
                    "            \"aggs\" : {\n" +
                    "                \"avg_age\" : {\n" +
                    "                    \"avg\" : { \"field\" : \"age\" }\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(queryStr);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "megacorp/employee/_search",
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
