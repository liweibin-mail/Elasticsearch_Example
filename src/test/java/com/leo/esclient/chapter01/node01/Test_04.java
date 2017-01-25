package com.leo.esclient.chapter01.node01;

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

    /**
     * 搜索年龄大于 30, 姓氏为 Smith 的雇员
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"bool\": {\n" +
                    "            \"must\": {\n" +
                    "                \"match\" : {\n" +
                    "                    \"last_name\" : \"smith\" \n" +
                    "                }\n" +
                    "            },\n" +
                    "            \"filter\": {\n" +
                    "                \"range\" : {\n" +
                    "                    \"age\" : { \"gt\" : 30 } \n" +
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

    /**
     * 全文搜索
     * 搜索所有喜欢攀岩（rock climbing）的雇员：
     * Elasticsearch 默认按照相关性得分排序，即每个文档跟查询的匹配程度。
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"match\" : {\n" +
                    "            \"about\" : \"rock climbing\"\n" +
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

    /**
     * 短语搜索
     * 找出一个属性中的独立单词是没有问题的，但有时候想要精确匹配一系列单词或者短语 。
     * 比如， 我们想执行这样一个查询，仅匹配同时包含 “rock” 和 “climbing” ，
     * 并且 二者以短语 “rock climbing” 的形式紧挨着的雇员记录。
     * 为此对 match 查询稍作调整，使用一个叫做 match_phrase 的查询：
     */
    @Test
    public void test_04() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"match_phrase\" : {\n" +
                    "            \"about\" : \"rock climbing\"\n" +
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

    /**
     * 高亮搜索：
     * 许多应用都倾向于在每个搜索结果中 高亮 部分文本片段，以便让用户知道为何该文档符合查询条件。
     * 在 Elasticsearch 中检索出高亮片段也很容易。
     * 再次执行前面的查询，并增加一个新的 highlight 参数：
     *
     * @throws IOException
     */
    @Test
    public void test_05() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String queryStr = "{\n" +
                    "    \"query\" : {\n" +
                    "        \"match_phrase\" : {\n" +
                    "            \"about\" : \"rock climbing\"\n" +
                    "        }\n" +
                    "    },\n" +
                    "    \"highlight\": {\n" +
                    "        \"fields\" : {\n" +
                    "            \"about\" : {}\n" +
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
