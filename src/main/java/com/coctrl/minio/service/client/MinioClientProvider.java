package com.coctrl.minio.service.client;

/**
 * 类 MinioClientProvider 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/12/01 17:18
 */
public interface MinioClientProvider {
    /**
     * 获取一个MinioClient
     *
     * @param endpoint  端点
     * @param accessKey accessKey
     * @param secretKey secretKey
     * @return MinioClient
     */
    ExtendMinioClient getClient(String endpoint, String accessKey, String secretKey);
}
