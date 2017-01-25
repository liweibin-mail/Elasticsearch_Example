package com.leo.esclient.chapter01.node03;

import com.leo.esclient.util.FormatUtil;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;

/**
 * 删除文档
 * Created by liweibin on 2017/1/25 0025.
 */
public class Test_13 {

    /**
     * 在删除时, 即使文档不存在（ Found 是 false ）， _version 值仍然会增加。
     * 这是 Elasticsearch 内部记录本的一部分，
     * 用来确保这些改变在跨多节点时以正确的顺序执行。
     * <p/>
     * 正如已经在更新整个文档中提到的，删除文档不会立即将文档从磁盘中删除，
     * 只是将文档标记为已删除状态。随着你不断的索引更多的数据，
     * Elasticsearch 将会在后台清理标记为已删除的文档。
     */
    @Test
    public void test_01() throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response indexResponse = restClient.performRequest(
                    "DELETE",
                    "/website/blog/123");
            System.out.println("------------out--------------");
            FormatUtil.printJson(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();
        }
    }
}
