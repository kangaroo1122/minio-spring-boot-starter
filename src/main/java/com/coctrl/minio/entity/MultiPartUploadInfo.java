package com.coctrl.minio.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 类 MultiPartUploadInfo 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2022/03/13 13:12
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MultiPartUploadInfo implements Serializable {
    private static final long serialVersionUID = 2351645551281431063L;

    private String uploadId;

    private String fileName;

    private LocalDateTime expiryTime;

    private List<String> uploadUrls;
}
