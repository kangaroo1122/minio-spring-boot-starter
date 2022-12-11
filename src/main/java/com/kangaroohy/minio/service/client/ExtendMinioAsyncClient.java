package com.kangaroohy.minio.service.client;

import com.google.common.collect.Multimap;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.Part;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

/**
 * 类 ExtendMinioClient 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/12/01 17:20
 */
public class ExtendMinioAsyncClient extends MinioAsyncClient {
    protected ExtendMinioAsyncClient(MinioAsyncClient client) {
        super(client);
    }

    /**
     * 获取uploadId
     *
     * @param bucketName
     * @param region
     * @param objectName
     * @param headers
     * @param extraQueryParams
     * @return
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     * @throws InsufficientDataException
     * @throws XmlParserException
     * @throws InternalException
     * @throws IOException
     */
    public String initMultiPartUpload(String bucketName, String region, String objectName,
                                      Multimap<String, String> headers, Multimap<String, String> extraQueryParams)
            throws InsufficientDataException, NoSuchAlgorithmException, IOException,
            InvalidKeyException, XmlParserException, InternalException, ExecutionException, InterruptedException {
        CreateMultipartUploadResponse response = this.createMultipartUploadAsync(bucketName, region, objectName, headers, extraQueryParams).get();
        return response.result().uploadId();
    }

    /**
     * 上传分片数据
     */
    public UploadPartResponse uploadPart(String bucketName, String region, String objectName, Object data, int length, String uploadId,
                                         int partNumber, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException,
            XmlParserException, InternalException, ExecutionException, InterruptedException {
        return this.uploadPartAsync(bucketName, region, objectName, data, length, uploadId, partNumber, extraHeaders, extraQueryParams).get();
    }

    /**
     * 完成分片上传
     */
    public ObjectWriteResponse mergeMultipartUpload(String bucketName, String region, String objectName, String uploadId,
                                                    Part[] parts, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException, XmlParserException,
            InternalException, InterruptedException, ExecutionException {
        return this.completeMultipartUploadAsync(bucketName, region, objectName, uploadId, parts, extraHeaders, extraQueryParams).get();
    }

    /**
     * 取消分片上传
     */
    public AbortMultipartUploadResponse removeMultipartUpload(String bucketName, String region, String objectName, String uploadId,
                                                              Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException, XmlParserException,
            InternalException, InterruptedException, ExecutionException {
        return this.abortMultipartUploadAsync(bucketName, region, objectName, uploadId, extraHeaders, extraQueryParams).get();
    }

    public ListPartsResponse listMultipart(String bucketName, String region, String objectName, Integer maxParts,
                                           Integer partNumberMarker, String uploadId, Multimap<String, String> extraHeaders,
                                           Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException, XmlParserException,
            InternalException, InterruptedException, ExecutionException {
        return this.listPartsAsync(bucketName, region, objectName, maxParts, partNumberMarker, uploadId, extraHeaders, extraQueryParams).get();
    }
}
