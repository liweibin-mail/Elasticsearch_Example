package com.leo.esclient.chapter01.node02;

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
 * Elasticsearch 的集群监控信息中包含了许多的统计数据，其
 * 中最为重要的一项就是 集群健康 ，
 * 它在 status 字段中展示为 green 、 yellow 或者 red 。
 */
public class Test_06 {

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
}
