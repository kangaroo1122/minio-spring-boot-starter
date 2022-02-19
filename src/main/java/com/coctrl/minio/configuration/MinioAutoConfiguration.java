package com.coctrl.minio.configuration;

import com.coctrl.minio.constant.MinioConstant;
import com.coctrl.minio.service.MinioService;
import com.coctrl.minio.service.client.ExtendMinioClient;
import com.coctrl.minio.service.client.MinioClientProvider;
import com.coctrl.minio.service.client.MinioClientProviderImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

/**
 * 类 MinioAutoConfiguration 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/11/28 00:15
 */
@Configuration
@EnableConfigurationProperties(MinioProperties.class)
@ConditionalOnClass(MinioService.class)
@ConditionalOnProperty(prefix = MinioConstant.PREFIX, value = "enabled", matchIfMissing = true)
public class MinioAutoConfiguration {

    private final MinioProperties properties;

    public MinioAutoConfiguration(MinioProperties properties){
        this.properties = properties;
    }

    @Bean(name = "minioClientProvider")
    @ConditionalOnMissingBean(MinioClientProvider.class)
    public MinioClientProvider minioClientProvider(){
        return new MinioClientProviderImpl();
    }

    @Bean
    @ConditionalOnMissingBean(ExtendMinioClient.class)
    @DependsOn("minioClientProvider")
    public ExtendMinioClient extendMinioClient() {
        return minioClientProvider().getClient(properties.getEndpoint(), properties.getAccessKey(), properties.getSecretKey());
    }

    @Bean
    @ConditionalOnMissingBean(MinioService.class)
    public MinioService minioService(ExtendMinioClient extendMinioClient){
        return new MinioService(properties, extendMinioClient);
    }
}
