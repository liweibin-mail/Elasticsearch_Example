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
 * 创建一个新文档
 * _index 、 _type 和 _id 的组合可以唯一标识一个文档
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_12 {

    /**
     * 第一种方法使用 op_type 查询 -字符串参数：
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"title\": \"My first blog entry\",\n" +
                    "  \"text\":  \"I am starting to get the hang of this...\",\n" +
                    "  \"date\":  \"2014/01/02\"\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/123?op_type=create", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }

    /**
     * 第二种方法是在 URL 末端使用 /_create :
     * @throws IOException
     */
    @Test
    public void test_02() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "  \"title\": \"My first blog entry\",\n" +
                    "  \"text\":  \"I am starting to get the hang of this...\",\n" +
                    "  \"date\":  \"2014/01/02\"\n" +
                    "}";
            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/website/blog/123/_create", Collections.<String, String>emptyMap(), entity);
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
