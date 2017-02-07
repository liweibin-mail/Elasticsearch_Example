package com.leo.esclient.chapter01.node07;

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
 * 组合多查询
 * 1)must 文档必须匹配这些条件才能被包含进来
 * 2)must_not 文档必须不匹配这些条件才能包含进来
 * 3)should 如果满足这些语句中的任意语句，将增加 _score，否则，无任何影响。它们主要用于修正每个文档的相关性得分
 * 4)filter 必须匹配，但它不评分，以过滤的模式来运行。这些语句对评分没有贡献，只是根据过滤标准来排除或包含文档。
 * Created by liweibin on 2017/2/7 0007.
 */
public class Test_23 {

    /**
     * @throws IOException
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "    \"query\": {\n" +
                    "       \"bool\": {\n" +
                    "            \"must\":     { \"match\": { \"title\": \"how to make millions\" }},\n" +
                    "            \"must_not\": { \"match\": { \"tag\":   \"spam\" }},\n" +
                    "            \"should\": [\n" +
                    "                 { \"match\": { \"tag\": \"starred\" }},\n" +
                    "                 { \"range\": { \"date\": { \"gte\": \"2014/01/01\" }}}\n" +
                    "            ]\n" +
                    "       }\n" +
                    "   }" +
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
     * 增加带过滤器的查询
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "    \"query\": {\n" +
                    "       \"bool\": {\n" +
                    "           \"must\":     { \"match\": { \"title\": \"how to make millions\" }},\n" +
                    "           \"must_not\": { \"match\": { \"tag\":   \"spam\" }},\n" +
                    "           \"should\": [\n" +
                    "               { \"match\": { \"tag\": \"starred\" }}\n" +
                    "           ],\n" +
                    "           \"filter\": {\n" +
                    "           \"range\": { \"date\": { \"gte\": \"2014-01-01\" }} \n" +
                    "               }\n" +
                    "           }\n" +
                    "       }" +
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
     * constant_score 查询
     * 尽管没有 bool 查询使用这么频繁，constant_score 查询也是你工具箱里有用的查询工具。
     * 它将一个不变的常量评分应用于所有匹配的文档。它被经常用于你只需要执行一个 filter
     * 而没有其它查询（例如，评分查询）的情况下。
     * 可以使用它来取代只有 filter 语句的 bool 查询。在性能上是完全相同的，
     * 但对于提高查询简洁性和清晰度有很大帮助。
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "    \"query\": {\n" +
                    "       \"constant_score\":   {\n" +
                    "           \"filter\": {\n" +
                    "               \"term\": { \"category\": \"ebooks\" } \n" +
                    "               }\n" +
                    "           }\n" +
                    "       }" +
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
}
