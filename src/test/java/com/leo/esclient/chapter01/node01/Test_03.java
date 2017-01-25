package com.leo.esclient.chapter01.node01;

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

/**
 * Created by liweibin on 2017/1/24 0024.
 */
public class Test_03 {

    @Test
    @Description("检索雇员信息")
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            HttpEntity entity = new NStringEntity("");
            Response indexResponse = restClient.performRequest(
                    "GET",
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

    /**
     * 返回结果包括了所有文档，放在数组 hits 中。一个搜索默认返回十条结果。
     */
    @Test
    @Description("轻量搜索")
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            HttpEntity entity = new NStringEntity("");
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
    @Description("搜索姓氏为 ``Smith`` 的雇员")
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            HttpEntity entity = new NStringEntity("");
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "megacorp/employee/_search?q=last_name:Smith",
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
