package com.kangaroohy.minio.configuration;

import com.kangaroohy.minio.constant.MinioConstant;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 类 MinioProperties 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/11/28 00:15
 */
@Data
@Component
@ConfigurationProperties(prefix = MinioConstant.PREFIX)
public class MinioProperties {

    /**
     * 连接地址
     */
    private String endpoint;

    /**
     * 用户名
     */
    private String accessKey;

    /**
     * 密码
     */
    private String secretKey;

    /**
     * 访问地址
     */
    private String address = "";

}
