package com.leo.esclient;

import com.alibaba.fastjson.JSON;
import com.leo.esclient.util.FormatUtil;
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
 * 初始化雇员信息
 * Created by liweibin on 2017/1/24 0024.
 */
public class Test_02 {

    @Test
    @Description("在ES中增加员工信息")
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Map<String, Object> nested = new HashMap<>();
            nested.put("match_all", new HashMap());
            Map<String, Object> employee1 = new HashMap<>();
            employee1.put("first_name", "Jane");
            employee1.put("last_name", "Smith");
            employee1.put("age", 25);
            employee1.put("about", "I love to go rock climbing");
            employee1.put("interests", new String[]{"sports", "music"});

            System.out.println("-----------------------------");
            System.out.println(JSON.toJSONString(employee1));
            System.out.println("-----------------------------");

            HttpEntity entity = new NStringEntity(
                    JSON.toJSONString(employee1), ContentType.APPLICATION_JSON);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "megacorp/employee/1",
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
    @Description("在ES中增加员工信息")
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Map<String, Object> nested = new HashMap<>();
            nested.put("match_all", new HashMap());
            Map<String, Object> employee1 = new HashMap<>();
            employee1.put("first_name", "Jane");
            employee1.put("last_name", "Smith");
            employee1.put("age", 32);
            employee1.put("about", "I like to collect rock albums");
            employee1.put("interests", new String[]{"music"});

            System.out.println("-----------------------------");
            System.out.println(JSON.toJSONString(employee1));
            System.out.println("-----------------------------");

            HttpEntity entity = new NStringEntity(
                    JSON.toJSONString(employee1), ContentType.APPLICATION_JSON);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "megacorp/employee/2",
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
    @Description("在ES中增加员工信息")
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Map<String, Object> nested = new HashMap<>();
            nested.put("match_all", new HashMap());
            Map<String, Object> employee1 = new HashMap<>();
            employee1.put("first_name", "Douglas");
            employee1.put("last_name", "Fir");
            employee1.put("age", 35);
            employee1.put("about", "I like to build cabinets");
            employee1.put("interests", new String[]{"forestry"});

            System.out.println("-----------------------------");
            System.out.println(JSON.toJSONString(employee1));
            System.out.println("-----------------------------");

            HttpEntity entity = new NStringEntity(
                    JSON.toJSONString(employee1), ContentType.APPLICATION_JSON);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "megacorp/employee/3",
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
