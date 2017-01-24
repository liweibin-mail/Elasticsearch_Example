package com.leo.esclient;

import com.alibaba.fastjson.JSON;
import com.sun.org.glassfish.gmbal.Description;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liweibin on 2017/1/24 0024.
 */
public class Test_01 {

    @Test
    @Description("计算集群中文档的数量")
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Map<String, Object> nested = new HashMap<>();
            nested.put("match_all", new HashMap());
            Map<String, Object> map = new HashMap<>();
            map.put("query", nested);

            System.out.println("-----------------------------");
            System.out.println(JSON.toJSONString(map));
            System.out.println("-----------------------------");

            HttpEntity entity = new NStringEntity(
                    JSON.toJSONString(map), ContentType.APPLICATION_JSON);
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "_count?pretty",
                    Collections.<String, String>emptyMap(),
                    entity);
            System.out.println("------------out--------------");
            System.out.println(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();

        }
    }
}
