package com.leo.esclient.chapter01.node03;

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
 * 文档部分更新:
 * 注：文档是不可变的,他们不能被修改,只能被替换
 * <p/>
 * 处理过程
 * 检索-修改-重建索引
 * <p/>
 * update API与put的区别在于这个过程发生在分片内部，
 * 这样就避免了多次请求的网络开销。通过减少检索和重建索引步骤之间的时间，
 * 我们也减少了其他进程的变更带来冲突的可能性。
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_15 {

    /**
     * update 请求最简单的一种形式是接收文档的一部分作为 doc 的参数，
     * 它只是与现有的文档进行合并。对象被合并到一起，覆盖现有的字段，增加新的字段。
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            //增加字段 tags/views 到博客文章 -- 可重复执行
            String query = "{\n" +
                    "   \"doc\" : {\n" +
                    "      \"tags\" : [ \"testing\" ],\n" +
                    "      \"views\": 1\n" +
                    "   }\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "POST",
                    "/website/blog/1/_update", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));

            //检索结果
            indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/1/_source", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 使用脚本部分更新文档
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "   \"script\" : \"ctx._source.views+=1\"\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);

            Response indexResponse = restClient.performRequest(
                    "POST",
                    "/website/blog/1/_update", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));

            //检索结果
            indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/1/_source", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
