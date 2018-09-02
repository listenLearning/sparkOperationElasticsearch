package com.elastic.spark;

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
     * 关闭客户端连接
     */
    private void close() {
        getClient().close();
    }

}
