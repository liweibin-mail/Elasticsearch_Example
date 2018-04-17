package com.leo.esclient;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;


public class App {
    public static void main(String[] args) throws IOException {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")).build();

            Response response = restClient.performRequest("GET", "/",
                    Collections.singletonMap("pretty", "true"));
            System.out.println(EntityUtils.toString(response.getEntity()));


            HttpEntity entity = new NStringEntity(
                    "{\n" +
                            "    \"user\" : \"kimchy\",\n" +
                            "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
                            "    \"message\" : \"trying out Elasticsearch\"\n" +
                            "}", ContentType.APPLICATION_JSON);
            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/twitter/tweet/1",
                    Collections.<String, String>emptyMap(),
                    entity);
            System.out.println(EntityUtils.toString(indexResponse.getEntity()));
        } finally {
            if (restClient != null)
                restClient.close();

        }
    }
}
