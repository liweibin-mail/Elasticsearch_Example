package com.leo.esclient.chapter01.node02;

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
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_07 {

    /**
     * 添加索引，并创建指定的分片和副本数
     * 当然，如果只是在相同节点数目的集群上增加更多的副本分片并不能提高性能，
     * 因为每个分片从节点上获得的资源会变少。
     * 你需要增加更多的硬件资源来提升吞吐量。
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            String query = "{\n" +
                    "   \"settings\" : {\n" +
                    "      \"number_of_shards\" : 3,\n" + //3个主分片
                    "      \"number_of_replicas\" : 1\n" + //每个主分片1个副本
                    "   }\n" +
                    "}";

            HttpEntity entity = new NStringEntity(query);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/blogs",
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
