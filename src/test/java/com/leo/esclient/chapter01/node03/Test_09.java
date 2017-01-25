package com.leo.esclient.chapter01.node03;

import com.leo.esclient.util.FormatUtil;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * 取回一个文档
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_09 {

    /**
     * 在请求的查询串参数中加上 pretty 参数，
     * 这将会调用 Elasticsearch 的 pretty-print 功能，该功能 使得 JSON 响应体更加可读。
     * 但是， _source 字段不能被格式化打印出来。相反，我们得到的 _source 字段中的 JSON 串，
     * 刚好是和我们传给它的一样。
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/123?pretty",
                    Collections.<String, String>emptyMap());
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 测试文档不存在的情况
     *
     * @throws IOException
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/124?pretty",
                    Collections.<String, String>emptyMap());
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 默认情况下， GET 请求 会返回整个文档，这个文档正如存储在 _source 字段中的一样。
     * 但是也许你只对其中的 title 字段感兴趣。单个字段能用 _source 参数请求得到，
     * 多个字段也能使用逗号分隔的列表来指定。
     *
     * @throws IOException
     */
    @Test
    public void test_03() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/123?pretty&_source=title,text",
                    Collections.<String, String>emptyMap());
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 只获取文档,不查询元数据的情况
     * @throws IOException
     */
    @Test
    public void test_04() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "GET",
                    "/website/blog/123/_source",
                    Collections.<String, String>emptyMap());
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
