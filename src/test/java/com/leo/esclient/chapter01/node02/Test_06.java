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
 * Elasticsearch 的集群监控信息中包含了许多的统计数据，其
 * 中最为重要的一项就是 集群健康 ，
 * 它在 status 字段中展示为
 * 1）green 所有的主分片和副本分片都正常运行。
 * 2）yellow 所有的主分片都正常运行，但不是所有的副本分片都正常运行。
 * 3）red 有主分片没能正常运行。
 *
 * 注：
 * unassigned_shards : 在同一个节点上既保存原始数据又保存副本是没有意义的，
 * 因为一旦失去了那个节点，我们也将丢失该节点上的所有副本数据
 */
public class Test_06 {

    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            HttpEntity entity = new NStringEntity("");
            Response indexResponse = restClient.performRequest(
                    "GET",
                    "_cluster/health",
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
