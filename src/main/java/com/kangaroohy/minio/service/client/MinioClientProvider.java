package com.kangaroohy.minio.service.client;

import io.minio.MinioClient;

/**
 * 类 MinioClientProvider 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/12/01 17:18
 */
public interface MinioClientProvider {
    /**
     * 获取一个MinioAsyncClient
     *
     * @param endpoint  端点
     * @param accessKey accessKey
     * @param secretKey secretKey
     * @return MinioClient
     */
    ExtendMinioAsyncClient getAsyncClient(String endpoint, String accessKey, String secretKey);
    /**
     * 获取一个MinioClient
     *
     * @param endpoint  端点
     * @param accessKey accessKey
     * @param secretKey secretKey
     * @return MinioClient
     */
    MinioClient getClient(String endpoint, String accessKey, String secretKey);
}
