package com.kangaroohy.minio.exceptions;

public class MinioServiceException extends Exception {
    protected MinioServiceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public MinioServiceException(Throwable cause) {
        super(cause);
    }

    public MinioServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public MinioServiceException(String message) {
        super(message);
    }

    public MinioServiceException() {
        super();
    }
}
