package com.coctrl.minio.service;

import com.coctrl.minio.configuration.MinioProperties;
import com.coctrl.minio.constant.MinioConstant;
import com.coctrl.minio.service.client.ExtendMinioClient;
import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 类 MinioService 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/11/28 00:15
 */
@Service
public class MinioService {
    public static final Logger logger = Logger.getLogger(MinioService.class);

    private final MinioProperties properties;

    private final ExtendMinioClient minioClient;

    public MinioService(MinioProperties properties, ExtendMinioClient minioClient) {
        this.properties = properties;
        this.minioClient = minioClient;
    }

    /**
     * 查看指定bucket是否存在
     *
     * @param bucketName bucket名称
     * @return
     */
    public boolean bucketExists(String bucketName) throws MinioException {
        try {
            return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 创建一个bucket
     *
     * @param bucketName bucket名称
     * @return
     */
    public boolean createBucket(String bucketName) throws MinioException {
        try {
            if (!bucketExists(bucketName)) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 获得所有bucket
     *
     * @return
     */
    public List<Bucket> listBuckets() throws MinioException {
        try {
            return minioClient.listBuckets();
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 获得指定bucket
     *
     * @param bucketName bucket名称
     * @return
     */
    public Optional<Bucket> getBucket(String bucketName) throws MinioException {
        return listBuckets().stream().filter(item -> item.name().equals(bucketName)).findFirst();
    }

    /**
     * 删除一个bucket
     *
     * @param bucketName bucket名称
     * @return
     */
    public boolean removeBucket(String bucketName) throws MinioException {
        try {
            if (bucketExists(bucketName)) {
                minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
            }
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 根据前缀获得文件
     *
     * @param bucketName bucket名称
     * @param prefix     前缀
     * @param recursive  是否递归查询
     * @return
     */
    public List<Item> listObjects(String bucketName, String prefix, boolean recursive) throws MinioException {
        List<Item> objectList = new ArrayList<>();
        Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(prefix).recursive(recursive).build());
        for (Result<Item> itemResult : results) {
            try {
                objectList.add(itemResult.get());
            } catch (ErrorResponseException | IOException | InsufficientDataException
                    | InternalException | InvalidKeyException | InvalidResponseException
                    | NoSuchAlgorithmException | XmlParserException | ServerException e) {
                throw new MinioException(e.getMessage());
            }
        }
        return objectList;
    }

    /**
     * 获得指定文件 文件流
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public InputStream getObject(String bucketName, String objectName) throws MinioException {
        InputStream inputStream = null;
        try {
            inputStream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
        return inputStream;
    }

    /**
     * 获得指定文件 文件流
     *
     * @param bucketName
     * @param objectName
     * @param versionId
     * @return
     */
    public InputStream getObject(String bucketName, String objectName, String versionId) throws MinioException {
        InputStream inputStream = null;
        try {
            inputStream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).versionId(versionId).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
        return inputStream;
    }

    /**
     * 获得指定文件 文件流
     *
     * @param bucketName
     * @param objectName
     * @param length
     * @param offset
     * @return
     */
    public InputStream getObject(String bucketName, String objectName, long length, Long offset) throws MinioException {
        InputStream inputStream = null;
        try {
            inputStream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).length(length).offset(offset).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
        return inputStream;
    }

    /**
     * 获得外链，过期时间默认7天
     *
     * @param bucketName bucket 名称
     * @param objectName 文件
     * @return
     */
    public String getObjectUrl(String bucketName, String objectName) throws MinioException {
        return getObjectUrl(bucketName, objectName, GetPresignedObjectUrlArgs.DEFAULT_EXPIRY_TIME);
    }

    /**
     * 获得外链
     *
     * @param bucketName bucket 名称
     * @param objectName 文件
     * @param expires    过期时间，单位秒
     * @return
     */
    public String getObjectUrl(String bucketName, String objectName, Integer expires) throws MinioException {
        try {
            return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName).object(objectName).expiry(expires).method(Method.GET).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 上传文件
     *
     * @param bucketName  bucket名称
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param contentType 文件类型
     * @param stream      文件流
     * @return
     */
    public ObjectWriteResponse putObject(String bucketName, String objectName, String contentType,
                                         InputStream stream) throws MinioException {
        try {
            return putObject(bucketName, objectName, contentType, stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE);
        } catch (IOException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 上传文件
     *
     * @param bucketName  bucket名称
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param contentType 文件类型
     * @param stream      文件流
     * @param objectSize  文件大小
     * @return
     */
    public ObjectWriteResponse putObject(String bucketName, String objectName, String contentType,
                                         InputStream stream, long objectSize) throws MinioException {
        return putObject(bucketName, objectName, contentType, stream, objectSize, -1);
    }

    /**
     * 上传文件
     *
     * @param bucketName  bucket名称
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param contentType 文件类型
     * @param stream      文件流
     * @param objectSize  文件大小
     * @param partSize    分片大小
     * @return
     */
    public ObjectWriteResponse putObject(String bucketName, String objectName, String contentType,
                                         InputStream stream, long objectSize, long partSize) throws MinioException {
        try {
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(stream, objectSize, partSize)
                    .contentType(contentType)
                    .build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 单个删除
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public boolean removeObject(String bucketName, String objectName) throws MinioException {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 批量删除
     *
     * @param bucketName
     * @param objectNames
     * @return
     */
    public List<String> removeObjects(String bucketName, Collection<String> objectNames) throws MinioException {
        List<DeleteObject> objects = objectNames.stream().map(DeleteObject::new).collect(Collectors.toList());
        Iterable<Result<DeleteError>> results = minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objects).build());
        List<String> errorDeleteObjects = new ArrayList<>();
        try {
            for (Result<DeleteError> result : results) {
                errorDeleteObjects.add(result.get().objectName());
                logger.error(String.format("Error in deleting object %s:%s, code=%s, message=%s",
                        bucketName, result.get().objectName(), result.get().code(), result.get().message()));
            }
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
        return errorDeleteObjects;
    }

    /**
     * 获取上传临时签名
     *
     * @param bucketName
     * @param fileName
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPolicy(String bucketName, String fileName) throws MinioException {
        return getPolicy(bucketName, fileName, ZonedDateTime.now().plusMinutes(10));
    }

    /**
     * 获取上传临时签名
     *
     * @param bucketName
     * @param fileName
     * @param time
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPolicy(String bucketName, String fileName, ZonedDateTime time) throws MinioException {
        return getPolicy(bucketName, null, fileName, time);
    }

    /**
     * 获取上传临时签名
     *
     * @param bucketName
     * @param path       文件保存路径，如：test/file
     * @param fileName
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPolicy(String bucketName, String path, String fileName) throws MinioException {
        return getPolicy(bucketName, path, fileName, ZonedDateTime.now().plusMinutes(10));
    }

    /**
     * 获取上传临时签名
     *
     * @param bucketName
     * @param path       文件保存路径，如：test/file
     * @param fileName
     * @param time
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPolicy(String bucketName, String path, String fileName, ZonedDateTime time) throws MinioException {
        PostPolicy postPolicy = new PostPolicy(bucketName, time);
        String key = MinioConstant.URI_DELIMITER + getPath(path) + fileName;
        postPolicy.addEqualsCondition("key", key);
        try {
            Map<String, String> map = minioClient.getPresignedPostFormData(postPolicy);
            HashMap<String, String> map1 = new HashMap<>();
            map.forEach((k, v) -> map1.put(k.replace("-", ""), v));
            map1.put("key", key);
            map1.put("host", properties.getEndpoint() + MinioConstant.URI_DELIMITER + bucketName);
            return map1;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 获取上传文件的url
     *
     * @param bucketName
     * @param objectName
     * @return
     * @throws MinioException
     */
    public String getPolicyUrl(String bucketName, String objectName) throws MinioException {
        return getPolicyUrl(bucketName, null, objectName, Method.PUT, 2, TimeUnit.MINUTES);
    }

    /**
     * 获取上传文件的url
     *
     * @param bucketName
     * @param path
     * @param objectName
     * @return
     * @throws MinioException
     */
    public String getPolicyUrl(String bucketName, String path, String objectName) throws MinioException {
        return getPolicyUrl(bucketName, path, objectName, Method.PUT, 2, TimeUnit.MINUTES);
    }

    /**
     * 获取上传文件的url
     *
     * @param bucketName
     * @param objectName
     * @param method
     * @param time
     * @param timeUnit
     * @return
     * @throws MinioException
     */
    public String getPolicyUrl(String bucketName, String path, String objectName, Method method, int time, TimeUnit timeUnit) throws MinioException {
        try {
            if (path != null && !"".equals(path)) {
                objectName = getPath(path) + objectName;
            }
            return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                    .method(method)
                    .bucket(bucketName)
                    .object(objectName)
                    .expiry(time, timeUnit).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                | InternalException | InvalidKeyException | InvalidResponseException
                | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 获得 当前日期分割目录，如：2021/11/28
     *
     * @return
     */
    public String getDatePath() {
        return String.join(MinioConstant.URI_DELIMITER, getDateFolder());
    }

    /**
     * 获得文件外链，永久外链，需要服务端设置指定bucket为全部可访问
     *
     * @param bucket    bucket名称
     * @param finalPath 路径及文件名，如 2021/11/20/test.zip
     * @return
     */
    public String gatewayUrl(String bucket, String finalPath) {
        return properties.getEndpoint() + MinioConstant.URI_DELIMITER + bucket + MinioConstant.URI_DELIMITER + finalPath;
    }

    /**
     * 获取年月日[2021, 11, 11]
     *
     * @return
     */
    private String[] getDateFolder() {
        String[] retVal = new String[3];

        LocalDate localDate = LocalDate.now();
        retVal[0] = localDate.getYear() + "";

        int month = localDate.getMonthValue();
        retVal[1] = month < 10 ? "0" + month : month + "";

        int day = localDate.getDayOfMonth();
        retVal[2] = day < 10 ? "0" + day : day + "";

        return retVal;
    }

    /**
     * 配置的路径以 / 开始，去掉第一个路径符
     *
     * @param path
     * @return
     */
    private String getPath(String path) {
        if (path == null || "".equals(path)) {
            return MinioConstant.URI_DELIMITER;
        }
        path = path.startsWith(MinioConstant.URI_DELIMITER) ? path.substring(path.indexOf(MinioConstant.URI_DELIMITER) + 1) : path;
        path = path.endsWith(MinioConstant.URI_DELIMITER) ? path : path + MinioConstant.URI_DELIMITER;
        return path;
    }
}
