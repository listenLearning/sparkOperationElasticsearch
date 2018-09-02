package com.elastic.spark;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import sun.rmi.transport.Transport;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class ElasticearchUtils {
    private static volatile ElasticearchUtils instance;
    private TransportClient client = null;
    private BulkProcessor processor = null;

    private ElasticearchUtils() {
    }

    public static ElasticearchUtils getInstance() {
        if (instance == null) {
            synchronized (ElasticearchUtils.class) {
                if (instance == null) {
                    instance = new ElasticearchUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 获取es客户端连接
     *
     * @return TransportClient
     */
    public TransportClient getClient() {
        try {
            if (client == null) {
                Settings settings = Settings.builder()
                        .put("cluster.name", "elasticsearch")
                        .put("client.transport.sniff", true)
                        .build();
                client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("master"), 9300));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 获取批处理操作器
     *
     * @return
     */
    public BulkProcessor getProcessor() {
        BulkProcessor processor = BulkProcessor.builder(
                getClient(),
                new BulkProcessor.Listener() {
                    public void beforeBulk(long executionId, BulkRequest request) {
                        System.out.println("请求数:" + request.numberOfActions());
                    }

                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        if (!response.hasFailures()) {
                            System.out.println("执行成功!");
                        } else {
                            System.out.println("执行失败!");
                        }
                    }

                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        System.out.println(failure.getMessage());
                    }
                }
        ).setBulkActions(10000) //设置提交批处理操作的请求阈值数
                .setBulkSize(new ByteSizeValue(15, ByteSizeUnit.MB)) //设置提交批处理操作的请求大小阀值
                .setFlushInterval(TimeValue.timeValueSeconds(5)) //设置刷新索引时间间隔
                .setConcurrentRequests(6) //设置并发处理线程个数
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) //设置回滚策略，等待时间100ms,retry次数为3次
                .build();
        return processor;
    }

    /**
     * 判断是否存在该索引
     *
     * @param indexName
     * @return
     */
    public Boolean isIndexExists(String indexName) {
        IndicesExistsRequestBuilder builder = getClient().admin()
                .indices()
                .prepareExists(indexName);
        IndicesExistsResponse response = builder.get();
        return response.isExists();
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @param shards
     * @param replicas
     * @param refresh
     * @return
     */
    public String createIndex(String indexName, Integer shards, Integer replicas, String refresh) {
        String result = "";
        try {
            if (isIndexExists(indexName)) {
            } else {
                CreateIndexRequestBuilder builder = getClient().admin().indices().prepareCreate(indexName);
                Map<String, Object> settings = new HashMap<String, Object>();
                settings.put("number_of_shards", shards);// 分片数
                settings.put("number_of_replicas", replicas);// 副本数
                settings.put("refresh_interval", refresh);// 刷新时间
                builder.setSettings(settings);

                builder.addMapping(indexName/*, getIndexSource(indexName)*/);
                CreateIndexResponse response = builder.get();
                result = response.isAcknowledged() ? "索引创建成功" : "索引创建失败";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    public String deleteIndex(String indexName) {
        String result = "";
        try {
            if (!isIndexExists(indexName)) {
                System.out.println("");
            } else {
                DeleteIndexRequestBuilder builder = client.admin().indices().prepareDelete(indexName);
                DeleteIndexResponse response = builder.get();
                result = response.isAcknowledged() ? "索引删除成功" : "索引删除失败";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 关闭客户端连接
     */
    private void close() {
        getClient().close();
    }

}
