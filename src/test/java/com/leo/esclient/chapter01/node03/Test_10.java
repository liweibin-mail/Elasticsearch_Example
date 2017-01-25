package com.leo.esclient.chapter01.node03;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_10 {

    /**
     * 检查文档是否存在
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "HEAD",
                    "/website/blog/123");
            System.out.println("------------out--------------");
            System.out.println(indexResponse.toString());

            indexResponse = restClient.performRequest(
                    "HEAD",
                    "/website/blog/124");
            System.out.println("------------out--------------");
            System.out.println(indexResponse.toString());
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
