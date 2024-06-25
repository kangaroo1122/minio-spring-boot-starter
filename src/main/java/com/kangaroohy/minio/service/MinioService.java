package com.kangaroohy.minio.service;

import com.google.common.collect.Multimap;
import com.kangaroohy.minio.configuration.MinioProperties;
import com.kangaroohy.minio.constant.MinioConstant;
import com.kangaroohy.minio.entity.MultiPartUploadInfo;
import com.kangaroohy.minio.enums.PolicyType;
import com.kangaroohy.minio.exceptions.MinioServiceException;
import com.kangaroohy.minio.service.client.ExtendMinioAsyncClient;
import com.kangaroohy.minio.utils.CustomUtil;
import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
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
@Slf4j
public class MinioService {
    private final MinioProperties properties;

    private final ExtendMinioAsyncClient minioAsyncClient;

    private final MinioClient minioClient;

    public MinioService(MinioProperties properties, ExtendMinioAsyncClient minioAsyncClient, MinioClient minioClient) {
        this.properties = properties;
        this.minioAsyncClient = minioAsyncClient;
        this.minioClient = minioClient;
    }

    /**
     * 查看指定bucket是否存在
     *
     * @param bucketName bucket名称
     * @return
     */
    public boolean bucketExists(String bucketName) throws MinioServiceException {
        try {
            return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Error checking if bucket exists: " + bucketName, e);
        }
    }

    /**
     * 创建一个bucket
     *
     * @param bucketName bucket名称
     * @return
     */
    public boolean createBucket(String bucketName) throws MinioServiceException {
        try {
            if (!bucketExists(bucketName)) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to create bucket: " + bucketName, e);
        }
    }

    /**
     * 创建一个bucket，并指定访问策略
     *
     * @param bucketName bucket名称
     * @param policyType 访问策略
     * @return
     */
    public boolean createBucket(String bucketName, PolicyType policyType) throws MinioServiceException {
        try {
            if (this.createBucket(bucketName)) {
                minioClient.setBucketPolicy(SetBucketPolicyArgs.builder().bucket(bucketName).config(PolicyType.getPolicy(policyType, bucketName)).build());
            }
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to create bucket: " + bucketName, e);
        }
    }

    /**
     * 创建一个bucket，并指定访问策略，默认三种不满足时，自定义可调用此方法
     *
     * @param bucketName bucket名称
     * @param policy     访问策略
     * @return
     */
    public boolean createBucket(String bucketName, String policy) throws MinioServiceException {
        try {
            if (this.createBucket(bucketName)) {
                minioClient.setBucketPolicy(SetBucketPolicyArgs.builder().bucket(bucketName).config(policy).build());
            }
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to create bucket: " + bucketName, e);
        }
    }

    /**
     * 指定访问策略
     *
     * @param bucketName bucket名称
     * @param policyType 访问策略
     * @return
     */
    public boolean setBucketPolicy(String bucketName, PolicyType policyType) throws MinioServiceException {
        try {
            if (!bucketExists(bucketName)) {
                throw new MinioServiceException(bucketName + " bucket does not exist.");
            }
            minioClient.setBucketPolicy(SetBucketPolicyArgs.builder().bucket(bucketName).config(PolicyType.getPolicy(policyType, bucketName)).build());
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to set bucket policy for bucket: " + bucketName, e);
        }
    }

    /**
     * 指定访问策略，默认三种不满足时，自定义可调用此方法
     *
     * @param bucketName bucket名称
     * @param policy     访问策略
     * @return
     */
    public boolean setBucketPolicy(String bucketName, String policy) throws MinioServiceException {
        try {
            if (!bucketExists(bucketName)) {
                throw new MinioServiceException(bucketName + " bucket does not exist.");
            }
            minioClient.setBucketPolicy(SetBucketPolicyArgs.builder().bucket(bucketName).config(policy).build());
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to set bucket policy for bucket: " + bucketName, e);
        }
    }

    /**
     * 获得所有bucket
     *
     * @return
     */
    public List<Bucket> listBuckets() throws MinioServiceException {
        try {
            return minioClient.listBuckets();
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to list buckets", e);
        }
    }

    /**
     * 获得指定bucket
     *
     * @param bucketName bucket名称
     * @return
     */
    public Optional<Bucket> getBucket(String bucketName) throws MinioServiceException {
        return listBuckets().stream().filter(item -> item.name().equals(bucketName)).findFirst();
    }

    /**
     * 删除一个bucket
     *
     * @param bucketName bucket名称
     * @return
     */
    public boolean removeBucket(String bucketName) throws MinioServiceException {
        try {
            if (bucketExists(bucketName)) {
                minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
            }
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to remove bucket: " + bucketName, e);
        }
    }

    /**
     * 遍历默认 bucketName 下文件夹名称
     *
     * @return
     */
    public List<Item> listObjects() throws MinioServiceException {
        return listObjects(getBucketName());
    }

    /**
     * 遍历指定 bucketName 下文件夹名称
     *
     * @param bucketName bucket名称
     * @return
     */
    public List<Item> listObjects(String bucketName) throws MinioServiceException {
        return listObjects(bucketName, false);
    }

    /**
     * 遍历文件
     *
     * @param recursive 是否递归查询，是，则会返回文件全路径，否，则返回第一级文件夹
     * @return
     */
    public List<Item> listObjects(boolean recursive) throws MinioServiceException {
        return listObjects(getBucketName(), recursive);
    }

    /**
     * 遍历文件
     *
     * @param bucketName bucket名称
     * @param recursive  是否递归查询，是，则会返回文件全路径，否，则返回第一级文件夹
     * @return
     */
    public List<Item> listObjects(String bucketName, boolean recursive) throws MinioServiceException {
        return listObjects(bucketName, null, recursive);
    }

    /**
     * 遍历文件
     *
     * @param bucketName bucket名称
     * @param prefix     前缀，包括路径
     * @return
     */
    public List<Item> listObjects(String bucketName, String prefix) throws MinioServiceException {
        return listObjects(bucketName, prefix, false);
    }

    /**
     * 遍历文件
     *
     * @param bucketName bucket名称
     * @param prefix     前缀，包括路径
     * @param recursive  是否递归查询
     * @return
     */
    public List<Item> listObjects(String bucketName, String prefix, boolean recursive) throws MinioServiceException {
        List<Item> objectList = new ArrayList<>();
        Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(prefix).recursive(recursive).build());
        for (Result<Item> itemResult : results) {
            try {
                objectList.add(itemResult.get());
            } catch (ErrorResponseException | IOException | InsufficientDataException
                     | InternalException | InvalidKeyException | InvalidResponseException
                     | NoSuchAlgorithmException | XmlParserException | ServerException e) {
                throw new MinioServiceException("Failed to list objects in bucket: " + bucketName, e);
            }
        }
        return objectList;
    }

    /**
     * 获得指定文件 文件流
     *
     * @param objectName
     * @return
     */
    public InputStream getObject(String objectName) throws MinioServiceException {
        return getObject(getBucketName(), objectName);
    }

    /**
     * 获得指定文件 文件流
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public InputStream getObject(String bucketName, String objectName) throws MinioServiceException {
        InputStream inputStream;
        try {
            inputStream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(CustomUtil.getObjectName(objectName)).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to get object: " + objectName + " from bucket: " + bucketName, e);
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
    public InputStream getObject(String bucketName, String objectName, String versionId) throws MinioServiceException {
        InputStream inputStream;
        try {
            inputStream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(CustomUtil.getObjectName(objectName)).versionId(versionId).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to get object: " + objectName + " from bucket: " + bucketName, e);
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
    public InputStream getObject(String bucketName, String objectName, long length, Long offset) throws MinioServiceException {
        InputStream inputStream;
        try {
            inputStream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(CustomUtil.getObjectName(objectName)).length(length).offset(offset).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to get object: " + objectName + " from bucket: " + bucketName, e);
        }
        return inputStream;
    }

    /**
     * 获得外链，过期时间默认7天
     *
     * @param objectName 文件
     * @return
     */
    public String getObjectUrl(String objectName) throws MinioServiceException {
        return getObjectUrl(getBucketName(), objectName);
    }

    /**
     * 获得外链，过期时间默认7天
     *
     * @param bucketName bucket 名称
     * @param objectName 文件
     * @return
     */
    public String getObjectUrl(String bucketName, String objectName) throws MinioServiceException {
        return getObjectUrl(bucketName, objectName, false);
    }

    /**
     * 获得外链，过期时间默认7天
     *
     * @param bucketName     bucket 名称
     * @param objectName     文件
     * @param replaceAddress 是否替换访问域名
     * @return
     */
    public String getObjectUrl(String bucketName, String objectName, boolean replaceAddress) throws MinioServiceException {
        return getObjectUrl(bucketName, objectName, replaceAddress, GetPresignedObjectUrlArgs.DEFAULT_EXPIRY_TIME);
    }

    /**
     * 获得外链
     *
     * @param bucketName     bucket 名称
     * @param objectName     文件
     * @param replaceAddress 是否替换访问域名
     * @param expires        过期时间，单位秒
     * @return
     */
    public String getObjectUrl(String bucketName, String objectName, boolean replaceAddress, Integer expires) throws MinioServiceException {
        try {
            String objectUrl = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName).object(CustomUtil.getObjectName(objectName)).expiry(expires).method(Method.GET).build());
            return replaceAddress && properties.getAddress() != null ? objectUrl.replace(properties.getEndpoint(), properties.getAddress()) : objectUrl;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to get presigned URL for object: " + objectName + " in bucket: " + bucketName, e);
        }
    }

    /**
     * 上传文件
     *
     * @param objectName 文件名称，如：2021/11/28/test.zip
     * @param stream     文件流
     * @return
     */
    public ObjectWriteResponse putObject(String objectName, InputStream stream) throws MinioServiceException {
        return putObject(getBucketName(), objectName, stream);
    }

    /**
     * 上传文件
     *
     * @param bucketName bucket名称
     * @param objectName 文件名称，如：2021/11/28/test.zip
     * @param stream     文件流
     * @return
     */
    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream) throws MinioServiceException {
        return putObject(bucketName, objectName, null, stream);
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
                                         InputStream stream) throws MinioServiceException {
        try {
            return putObject(bucketName, objectName, contentType, stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE);
        } catch (IOException e) {
            throw new MinioServiceException("Failed to upload object: " + objectName + " to bucket: " + bucketName, e);
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
                                         InputStream stream, long objectSize) throws MinioServiceException {
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
                                         InputStream stream, long objectSize, long partSize) throws MinioServiceException {
        try {
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(CustomUtil.getObjectName(objectName))
                    .stream(stream, objectSize, partSize)
                    .contentType(CustomUtil.getContentType(contentType))
                    .build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to upload object: " + objectName + " to bucket: " + bucketName, e);
        }
    }


    /**
     * 上传大型对象的一部分到Minio存储桶，作为多部分上传过程的一部分。
     * 当对象大小超过单次上传限制时，通常使用此方法。
     *
     * @param region           存储桶所在的区域。
     * @param objectName       要上传的对象名称。
     * @param data             要上传的对象部分的数据内容。
     * @param length           要上传的数据长度，单位为字节。
     * @param uploadId         唯一标识多部分上传会话的上传ID。initMultiPartUpload()
     * @param partNumber       该多部分上传中的部分序号。每个部分都有一个唯一的序号。
     * @param extraHeaders     上传请求中可选的额外HTTP头信息。
     * @param extraQueryParams 上传请求中可选的额外HTTP查询参数。
     * @throws MinioServiceException 如果在上传过程中发生任何错误。
     */
    public void uploadPart(String region, String objectName, Object data, int length, String uploadId,
                           int partNumber, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws MinioServiceException {
        uploadPart(getBucketName(), region, objectName, data, length, uploadId, partNumber, extraHeaders, extraQueryParams);
    }

    /**
     * 上传大型对象的一部分到Minio存储桶，作为多部分上传过程的一部分。
     * 当对象大小超过单次上传限制时，通常使用此方法。
     *
     * @param bucketName       存储桶的名称，该桶用于存放上传的对象。
     * @param region           存储桶所在的区域。
     * @param objectName       要上传的对象名称。
     * @param data             要上传的对象部分的数据内容。
     * @param length           要上传的数据长度，单位为字节。
     * @param uploadId         唯一标识多部分上传会话的上传ID。initMultiPartUpload()
     * @param partNumber       该多部分上传中的部分序号。每个部分都有一个唯一的序号。
     * @param extraHeaders     上传请求中可选的额外HTTP头信息。
     * @param extraQueryParams 上传请求中可选的额外HTTP查询参数。
     * @throws MinioServiceException 如果在上传过程中发生任何错误。
     */
    public void uploadPart(String bucketName, String region, String objectName, Object data, int length, String uploadId,
                           int partNumber, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams) throws MinioServiceException {
        try {
            minioAsyncClient.uploadPart(bucketName, region, objectName, data, length, uploadId, partNumber, extraHeaders, extraQueryParams);
        } catch (IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | NoSuchAlgorithmException | XmlParserException |
                 ExecutionException |
                 InterruptedException e) {
            throw new MinioServiceException("Failed to upload object: " + objectName + " to bucket: " + bucketName, e);
        }
    }

    /**
     * 单个删除
     *
     * @param objectName
     * @return
     */
    public boolean removeObject(String objectName) throws MinioServiceException {
        return removeObject(getBucketName(), objectName);
    }

    /**
     * 单个删除
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public boolean removeObject(String bucketName, String objectName) throws MinioServiceException {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(CustomUtil.getObjectName(objectName)).build());
            return true;
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to remove object: " + objectName + " from bucket: " + bucketName, e);
        }
    }

    /**
     * 批量删除
     *
     * @param bucketName
     * @param objectNames
     * @return
     */
    public List<String> removeObjects(String bucketName, Collection<String> objectNames) throws MinioServiceException {
        List<DeleteObject> objects = objectNames.stream().map(CustomUtil::getObjectName).map(DeleteObject::new).collect(Collectors.toList());
        Iterable<Result<DeleteError>> results = minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objects).build());
        List<String> errorDeleteObjects = new ArrayList<>();
        try {
            for (Result<DeleteError> result : results) {
                errorDeleteObjects.add(result.get().objectName());
                log.error(String.format("Error in deleting object %s:%s, code=%s, message=%s",
                        bucketName, result.get().objectName(), result.get().code(), result.get().message()));
            }
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to remove objects from bucket: " + bucketName, e);
        }
        return errorDeleteObjects;
    }

    /**
     * 获取上传临时签名
     *
     * @param fileName
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPresignedPostFormData(String fileName) throws MinioServiceException {
        return getPresignedPostFormData(getBucketName(), fileName);
    }

    /**
     * 获取上传临时签名
     *
     * @param bucketName
     * @param fileName
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPresignedPostFormData(String bucketName, String fileName) throws MinioServiceException {
        return getPresignedPostFormData(bucketName, fileName, ZonedDateTime.now().plusMinutes(10));
    }

    /**
     * 获取上传临时签名
     *
     * @param bucketName
     * @param fileName
     * @param time       默认10分钟以内有效
     * @return
     * @throws MinioException
     */
    public Map<String, String> getPresignedPostFormData(String bucketName, String fileName, ZonedDateTime time) throws MinioServiceException {
        return getPresignedPostFormData(bucketName, null, fileName, time);
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
    public Map<String, String> getPresignedPostFormData(String bucketName, String path, String fileName) throws MinioServiceException {
        return getPresignedPostFormData(bucketName, path, fileName, ZonedDateTime.now().plusMinutes(10));
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
    public Map<String, String> getPresignedPostFormData(String bucketName, String path, String fileName, ZonedDateTime time) throws MinioServiceException {
        PostPolicy postPolicy = new PostPolicy(bucketName, time);
        String key = MinioConstant.URI_DELIMITER + CustomUtil.getPath(path) + fileName;
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
            throw new MinioServiceException("Failed to get presigned POST form data for file: " + fileName + " in bucket: " + bucketName, e);
        }
    }

    /**
     * 获取上传文件的url
     *
     * @param objectName
     * @return
     * @throws MinioException
     */
    public String getPresignedObjectPutUrl(String objectName) throws MinioServiceException {
        return getPresignedObjectPutUrl(getBucketName(), objectName);
    }

    /**
     * 获取上传文件的url
     *
     * @param bucketName
     * @param objectName
     * @return
     * @throws MinioException
     */
    public String getPresignedObjectPutUrl(String bucketName, String objectName) throws MinioServiceException {
        return getPresignedObjectPutUrl(bucketName, null, objectName);
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
    public String getPresignedObjectPutUrl(String bucketName, String path, String objectName) throws MinioServiceException {
        return getPresignedObjectPutUrl(bucketName, path, objectName, 5);
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
    public String getPresignedObjectPutUrl(String bucketName, String path, String objectName, Integer time) throws MinioServiceException {
        return getPresignedObjectPutUrl(bucketName, path, objectName, time, TimeUnit.MINUTES);
    }

    /**
     * 获取上传文件的url
     *
     * @param bucketName
     * @param path
     * @param objectName
     * @param time
     * @param timeUnit
     * @return
     * @throws MinioException
     */
    public String getPresignedObjectPutUrl(String bucketName, String path, String objectName, Integer time, TimeUnit timeUnit) throws MinioServiceException {
        try {
            objectName = (StringUtils.hasText(path) ? CustomUtil.getPath(path) : "") + CustomUtil.getObjectName(objectName);
            return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                    .method(Method.PUT)
                    .bucket(bucketName)
                    .object(objectName)
                    .expiry(time, timeUnit).build());
        } catch (ErrorResponseException | IOException | InsufficientDataException
                 | InternalException | InvalidKeyException | InvalidResponseException
                 | NoSuchAlgorithmException | XmlParserException | ServerException e) {
            throw new MinioServiceException("Failed to get presigned PUT URL for object: " + objectName + " in bucket: " + bucketName, e);
        }
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param objectName
     * @param partSize
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String objectName, Integer partSize) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(getBucketName(), objectName, partSize);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param objectName
     * @param partSize
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String objectName, Integer partSize, String contentType) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(getBucketName(), objectName, partSize, contentType);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param bucketName
     * @param objectName
     * @param partSize
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String bucketName, String objectName, Integer partSize) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(bucketName, null, objectName, partSize);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param bucketName
     * @param objectName
     * @param partSize
     * @param contentType
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String bucketName, String objectName, Integer partSize, String contentType) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(bucketName, null, objectName, partSize, contentType);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param bucketName
     * @param path
     * @param objectName
     * @param partSize
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String bucketName, String path, String objectName, Integer partSize) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(bucketName, path, objectName, partSize, null);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param bucketName
     * @param path
     * @param objectName
     * @param partSize
     * @param contentType
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String bucketName, String path, String objectName, Integer partSize, String contentType) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(bucketName, path, objectName, partSize, contentType, 10);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param bucketName
     * @param path
     * @param objectName
     * @param partSize
     * @param contentType
     * @param time
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String bucketName, String path, String objectName, Integer partSize, String contentType, Integer time) throws MinioServiceException {
        return getPresignedMultipartUploadUrls(bucketName, path, objectName, partSize, contentType, time, TimeUnit.MINUTES);
    }

    /**
     * 获得分片上传的地址信息
     *
     * @param bucketName
     * @param path
     * @param objectName
     * @param partSize
     * @param contentType
     * @param time
     * @param timeUnit
     * @return
     * @throws MinioException
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrls(String bucketName, String path, String objectName, Integer partSize, String contentType, Integer time, TimeUnit timeUnit) throws MinioServiceException {
        String uploadId = "";
        List<String> partUrlList = new ArrayList<>();
        try {
            objectName = (StringUtils.hasText(path) ? CustomUtil.getPath(path) : "") + CustomUtil.getObjectName(objectName);
            InitiateMultipartUploadResult uploadResponse = this.initMultiPartUpload(bucketName, objectName, contentType);
            uploadId = uploadResponse.uploadId();
            Map<String, String> paramsMap = new HashMap<>(2);
            paramsMap.put("uploadId", uploadId);
            for (int i = 1; i <= partSize; i++) {
                paramsMap.put("partNumber", String.valueOf(i));
                // 获取上传 url
                String uploadUrl = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                        // 注意此处指定请求方法为 PUT，前端需对应，否则会报 `SignatureDoesNotMatch` 错误
                        .method(Method.PUT)
                        .bucket(bucketName)
                        .object(objectName)
                        // 指定上传连接有效期
                        .expiry(time, timeUnit)
                        .extraQueryParams(paramsMap).build());
                partUrlList.add(uploadUrl);
            }
        } catch (IOException | InsufficientDataException | InternalException | InvalidKeyException |
                 NoSuchAlgorithmException | XmlParserException | ErrorResponseException | InvalidResponseException |
                 ServerException e) {
            throw new MinioServiceException("Failed to get presigned multipart upload URLs for object: " + objectName + " in bucket: " + bucketName, e);
        }

        return MultiPartUploadInfo.builder()
                .uploadId(uploadId)
                .fileName(objectName)
                .expiryTime(CustomUtil.getLocalDateTime(time, timeUnit))
                .uploadUrls(partUrlList)
                .build();
    }

    /**
     * 根据分片partNumber获取前端分片上传预签名urls
     *
     * @param uploadId    上传标识，同一个文件，需保证一样，通过 initMultiPartUpload 方法获取
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param partNumbers 分片partNumber集合
     * @return
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrlsByPartNumbers(String uploadId, String objectName, List<Integer> partNumbers) throws MinioServiceException {
        return getPresignedMultipartUploadUrlsByPartNumbers(uploadId, getBucketName(), objectName, partNumbers);
    }

    /**
     * 根据分片partNumber获取前端分片上传预签名urls
     *
     * @param uploadId    上传标识，同一个文件，需保证一样，通过 initMultiPartUpload 方法获取
     * @param bucketName  bucketName
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param partNumbers 分片partNumber集合
     * @return
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrlsByPartNumbers(String uploadId, String bucketName, String objectName, List<Integer> partNumbers) throws MinioServiceException {
        return getPresignedMultipartUploadUrlsByPartNumbers(uploadId, bucketName, null, objectName, partNumbers);
    }

    /**
     * 根据分片partNumber获取前端分片上传预签名urls
     *
     * @param uploadId    上传标识，同一个文件，需保证一样，通过 initMultiPartUpload 方法获取
     * @param bucketName  bucketName
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param partNumbers 分片partNumber集合
     * @return
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrlsByPartNumbers(String uploadId, String bucketName, String path, String objectName, List<Integer> partNumbers) throws MinioServiceException {
        return getPresignedMultipartUploadUrlsByPartNumbers(uploadId, bucketName, path, objectName, partNumbers, 10);
    }

    /**
     * 根据分片partNumber获取前端分片上传预签名urls
     *
     * @param uploadId    上传标识，同一个文件，需保证一样，通过 initMultiPartUpload 方法获取
     * @param bucketName  bucketName
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param partNumbers 分片partNumber集合
     * @param time        过期时间，默认10分钟
     * @return
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrlsByPartNumbers(String uploadId, String bucketName, String path, String objectName, List<Integer> partNumbers, Integer time) throws MinioServiceException {
        return getPresignedMultipartUploadUrlsByPartNumbers(uploadId, bucketName, path, objectName, partNumbers, time, TimeUnit.MINUTES);
    }

    /**
     * 根据分片partNumber获取前端分片上传预签名urls
     *
     * @param uploadId    上传标识，同一个文件，需保证一样，通过 initMultiPartUpload 方法获取
     * @param bucketName  bucketName
     * @param objectName  文件名称，如：2021/11/28/test.zip
     * @param partNumbers 分片partNumber集合
     * @param time        过期时间，默认10分钟
     * @param timeUnit    过期时间单位，默认分钟
     * @return
     */
    public MultiPartUploadInfo getPresignedMultipartUploadUrlsByPartNumbers(String uploadId, String bucketName, String path, String objectName, List<Integer> partNumbers, Integer time, TimeUnit timeUnit) throws MinioServiceException {
        List<String> partUrlList = new ArrayList<>();
        try {
            objectName = (StringUtils.hasText(path) ? CustomUtil.getPath(path) : "") + CustomUtil.getObjectName(objectName);
            Map<String, String> paramsMap = new HashMap<>(2);
            paramsMap.put("uploadId", uploadId);
            for (Integer partNumber : partNumbers) {
                paramsMap.put("partNumber", String.valueOf(partNumber));
                // 获取上传 url
                String uploadUrl = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                        // 注意此处指定请求方法为 PUT，前端需对应，否则会报 `SignatureDoesNotMatch` 错误
                        .method(Method.PUT)
                        .bucket(bucketName)
                        .object(objectName)
                        // 指定上传连接有效期
                        .expiry(time, timeUnit)
                        .extraQueryParams(paramsMap).build());
                partUrlList.add(uploadUrl);
            }
        } catch (IOException | InsufficientDataException | InternalException | InvalidKeyException |
                 NoSuchAlgorithmException | XmlParserException | ErrorResponseException | InvalidResponseException |
                 ServerException e) {
            throw new MinioServiceException("Failed to get presigned multipart upload URLs for object: " + objectName + " in bucket: " + bucketName, e);
        }

        return MultiPartUploadInfo.builder()
                .uploadId(uploadId)
                .fileName(objectName)
                .expiryTime(CustomUtil.getLocalDateTime(time, timeUnit))
                .uploadUrls(partUrlList)
                .build();
    }

    /**
     * 初始化分片上传
     *
     * @param objectName 文件名称
     * @return
     * @throws MinioException
     */
    public InitiateMultipartUploadResult initMultiPartUpload(String objectName) throws MinioServiceException {
        return initMultiPartUpload(getBucketName(), objectName);
    }

    /**
     * 初始化分片上传
     *
     * @param bucketName bucketName
     * @param objectName 文件名称
     * @return
     * @throws MinioException
     */
    public InitiateMultipartUploadResult initMultiPartUpload(String bucketName, String objectName) throws MinioServiceException {
        return initMultiPartUpload(bucketName, null, objectName);
    }

    /**
     * 初始化分片上传
     *
     * @param bucketName bucketName
     * @param objectName 文件名称
     * @return
     * @throws MinioException
     */
    public InitiateMultipartUploadResult initMultiPartUpload(String bucketName, String path, String objectName) throws MinioServiceException {
        return initMultiPartUpload(bucketName, path, objectName, "application/octet-stream");
    }

    /**
     * 初始化分片上传
     *
     * @param bucketName  bucketName
     * @param objectName  文件名称
     * @param contentType contentType
     * @return
     * @throws MinioException
     */
    public InitiateMultipartUploadResult initMultiPartUpload(String bucketName, String path, String objectName, String contentType) throws MinioServiceException {
        try {
            objectName = (StringUtils.hasText(path) ? CustomUtil.getPath(path) : "") + CustomUtil.getObjectName(objectName);
            return minioAsyncClient.initMultiPartUpload(bucketName, null, objectName, CustomUtil.getHeader(contentType), null);
        } catch (IOException | InsufficientDataException | InternalException | InvalidKeyException |
                 NoSuchAlgorithmException | XmlParserException | ExecutionException | InterruptedException e) {
            throw new MinioServiceException("Failed to initiate multipart upload for object: " + objectName + " in bucket: " + bucketName, e);
        }
    }

    /**
     * 合并分片
     *
     * @param objectName
     * @param uploadId
     * @return
     * @throws MinioException
     */
    public String mergeMultiPartUpload(String objectName, String uploadId) throws MinioServiceException {
        return mergeMultiPartUpload(getBucketName(), objectName, uploadId);
    }

    /**
     * 合并分片
     *
     * @param bucketName
     * @param objectName
     * @param uploadId
     * @return
     * @throws MinioException
     */
    public String mergeMultiPartUpload(String bucketName, String objectName, String uploadId) throws MinioServiceException {
        return mergeMultiPartUpload(bucketName, objectName, uploadId, 1000);
    }


    /**
     * 合并分片
     *
     * @param bucketName
     * @param objectName
     * @param uploadId
     * @return
     * @throws MinioException
     */
    public String mergeMultiPartUpload(String bucketName, String objectName, String uploadId, Integer maxParts) throws MinioServiceException {
        try {
            ListPartsResponse partsResponse = minioAsyncClient.listMultipart(bucketName, null, CustomUtil.getObjectName(objectName), maxParts, 0, uploadId, null, null);
            if (null == partsResponse) {
                throw new MinioServiceException("No parts response available for object: " + objectName + " in bucket: " + bucketName + ", upload ID: " + uploadId);
            }
            List<Part> partList = partsResponse.result().partList();
            Part[] parts = new Part[partList.size()];
            partList.toArray(parts);
            ObjectWriteResponse writeResponse = minioAsyncClient.mergeMultipartUpload(bucketName, null, CustomUtil.getObjectName(objectName), uploadId, parts, null, null);
            if (null == writeResponse) {
                throw new MinioServiceException("Failed to complete multipart upload for object: " + objectName + " in bucket: " + bucketName + ", upload ID: " + uploadId);
            }
            return getAddress(writeResponse.region());
        } catch (IOException | InsufficientDataException | InternalException |
                 InvalidKeyException | NoSuchAlgorithmException | XmlParserException |
                 ExecutionException | InterruptedException e) {
            throw new MinioServiceException("Failed to merge multipart upload for object: " + objectName + " in bucket: " + bucketName + ", upload ID: " + uploadId, e);

        }
    }

    /**
     * 获取已上传的分片列表
     *
     * @param objectName
     * @param uploadId
     * @return
     * @throws MinioException
     */
    public List<Integer> listUploadMultiPart(String objectName, String uploadId) throws MinioServiceException {
        return listUploadMultiPart(getBucketName(), objectName, uploadId);
    }

    /**
     * 获取已上传的分片列表
     *
     * @param bucketName
     * @param objectName
     * @param uploadId
     * @return
     * @throws MinioException
     */
    public List<Integer> listUploadMultiPart(String bucketName, String objectName, String uploadId) throws MinioServiceException {
        return listUploadMultiPart(bucketName, objectName, uploadId, 1000);
    }

    /**
     * 获取已上传的分片列表
     *
     * @param bucketName
     * @param objectName
     * @param uploadId
     * @param maxParts
     * @return
     * @throws MinioException
     */
    public List<Integer> listUploadMultiPart(String bucketName, String objectName, String uploadId, Integer maxParts) throws MinioServiceException {
        ListPartsResponse partsResponse;
        try {
            partsResponse = minioAsyncClient.listMultipart(bucketName, null, CustomUtil.getObjectName(objectName), maxParts, 0, uploadId, null, null);
        } catch (NoSuchAlgorithmException | IOException | InvalidKeyException | ExecutionException |
                 InterruptedException | InsufficientDataException | XmlParserException | InternalException e) {
            throw new MinioServiceException("Failed to list multipart upload parts for object: " + objectName + " in bucket: " + bucketName + ", upload ID: " + uploadId, e);
        }
        if (null == partsResponse) {
            return Collections.emptyList();
        }
        return partsResponse.result().partList().stream().map(Part::partNumber).collect(Collectors.toList());
    }

    /**
     * 获得 当前日期分割目录，如：2021/11/28
     *
     * @return
     */
    public String getDatePath() {
        return String.join(MinioConstant.URI_DELIMITER, CustomUtil.getDateFolder());
    }

    /**
     * 获得文件外链，永久外链，需要服务端设置指定bucket为全部可访问
     *
     * @param bucket    bucket名称
     * @param finalPath 路径及文件名，如 2021/11/20/test.zip
     * @return
     */
    public String gatewayUrl(String bucket, String finalPath) {
        finalPath = finalPath.startsWith(MinioConstant.URI_DELIMITER) ? finalPath.substring(finalPath.indexOf(MinioConstant.URI_DELIMITER) + 1) : finalPath;
        return getAddress(properties.getEndpoint() + MinioConstant.URI_DELIMITER + bucket + MinioConstant.URI_DELIMITER + finalPath);
    }

    /**
     * 获得文件访问地址
     *
     * @return
     */
    public String getAddress(String url) {
        String address = "".equals(properties.getAddress().trim()) ? properties.getEndpoint() : properties.getAddress();
        return url.replace(properties.getEndpoint(), address);
    }

    /**
     * 默认BucketName
     *
     * @return
     */
    public String getBucketName() {
        if (!StringUtils.hasText(properties.getBucketName())) {
            throw new RuntimeException("未配置默认 BucketName");
        }
        return properties.getBucketName();
    }

}
