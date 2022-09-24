package com.kangaroohy.minio.service.client;

import com.google.common.collect.Multimap;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.Part;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * 类 ExtendMinioClient 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2021/12/01 17:20
 */
public class ExtendMinioClient extends MinioClient {
    protected ExtendMinioClient(MinioClient client) {
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
     * @throws ServerException
     * @throws XmlParserException
     * @throws ErrorResponseException
     * @throws InternalException
     * @throws InvalidResponseException
     * @throws IOException
     */
    public String getUploadId(String bucketName, String region, String objectName,
                              Multimap<String, String> headers, Multimap<String, String> extraQueryParams)
            throws ServerException, InsufficientDataException, ErrorResponseException, NoSuchAlgorithmException, IOException,
            InvalidKeyException, XmlParserException, InvalidResponseException, InternalException {
        CreateMultipartUploadResponse response = createMultipartUpload(bucketName, region, objectName, headers, extraQueryParams);
        return response.result().uploadId();
    }

    /**
     * 上传分片数据
     */
    public UploadPartResponse uploadPart(String bucketName, String region, String objectName, Object data, int length, String uploadId,
                                         int partNumber, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException,
            ServerException, XmlParserException, ErrorResponseException, InternalException, InvalidResponseException {
        return super.uploadPart(bucketName, region, objectName, data, length, uploadId, partNumber, extraHeaders, extraQueryParams);
    }

    /**
     * 完成分片上传
     */
    @Override
    public ObjectWriteResponse completeMultipartUpload(String bucketName, String region, String objectName, String uploadId,
                                                       Part[] parts, Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException,
            ServerException, XmlParserException, ErrorResponseException, InternalException, InvalidResponseException {
        return super.completeMultipartUpload(bucketName, region, objectName, uploadId, parts, extraHeaders, extraQueryParams);
    }

    /**
     * 取消分片上传
     */
    @Override
    public AbortMultipartUploadResponse abortMultipartUpload(String bucketName, String region, String objectName, String uploadId,
                                                             Multimap<String, String> extraHeaders, Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException,
            ServerException, XmlParserException, ErrorResponseException, InternalException, InvalidResponseException {
        return super.abortMultipartUpload(bucketName, region, objectName, uploadId, extraHeaders, extraQueryParams);
    }

    @Override
    public ListPartsResponse listParts(String bucketName, String region, String objectName, Integer maxParts,
                                       Integer partNumberMarker, String uploadId, Multimap<String, String> extraHeaders,
                                       Multimap<String, String> extraQueryParams)
            throws NoSuchAlgorithmException, InsufficientDataException, IOException, InvalidKeyException,
            ServerException, XmlParserException, ErrorResponseException, InternalException, InvalidResponseException {
        return super.listParts(bucketName, region, objectName, maxParts, partNumberMarker, uploadId, extraHeaders,
                extraQueryParams);
    }
}
