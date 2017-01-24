package com.leo.esclient;

import com.alibaba.fastjson.JSON;
import com.leo.esclient.util.FormatUtil;
import com.sun.org.glassfish.gmbal.Description;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
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
public class Test_04 {

    @Test
    @Description("使用查询表达式搜索")
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Map<String, Object> match = new HashMap<>();
            match.put("last_name", "Smith");

            Map<String, Object> query = new HashMap<>();
            query.put("match", match);

            Map<String, Object> json = new HashMap<>();
            json.put("query", query);

            HttpEntity entity = new NStringEntity(JSON.toJSONString(json));
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

    @Test
    @Description("")
    public void test_02() {

    }
}
