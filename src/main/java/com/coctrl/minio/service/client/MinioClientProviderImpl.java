package com.coctrl.minio.service.client;

import io.minio.MinioClient;
import org.springframework.stereotype.Service;

/**
 * 类 MinioClientProviderImpl 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/12/01 17:19
 */
@Service
public class MinioClientProviderImpl implements MinioClientProvider {

    private volatile ExtendMinioClient minioClient;

    @Override
    public ExtendMinioClient getClient(String endpoint, String accessKey, String secretKey) {
        // 通过double check来创建单例的Client
        if (minioClient == null) {
            synchronized (MinioClientProviderImpl.class) {
                if (minioClient == null) {
                    minioClient = new ExtendMinioClient(MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build());
                }
            }
        }
        return minioClient;
    }
}
