package com.github.sanjay.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){
        String hostname = "kafka-course-1-2974987369.us-west-2.bonsaisearch.net:443";
        //String hostname = "kafka-course-1-2974987369.us-west-2.bonsaisearch.net";
        String bonsaiURI = "https://iu6pxi3peo:6z7tb7ni1l@kafka-course-1-2974987369.us-west-2.bonsaisearch.net:443";
        String username = "iu6pxi3peo";
        String password = "6z7tb7ni1l";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username,password));

        System.out.println(hostname);

        URI connUri = URI.create(bonsaiURI);
        System.out.println(connUri.getHost());

        RestClientBuilder builder = RestClient.builder(
          //new HttpHost(hostname, 443,"https"))
                new HttpHost(connUri.getHost(), connUri.getPort(),connUri.getScheme()))
          .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                  return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
          }
        );
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        String jsonString ="{\"foo\": \"bar\"}";

        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets"
        ).source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);
        client.close();
    }
}
