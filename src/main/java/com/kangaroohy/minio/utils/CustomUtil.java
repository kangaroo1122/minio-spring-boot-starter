package com.kangaroohy.minio.utils;

import com.google.common.collect.HashMultimap;
import com.kangaroohy.minio.constant.MinioConstant;

import java.time.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 类 CustomUtil 功能描述：<br/>
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2023/4/11 12:22
 */
public class CustomUtil {

    private CustomUtil() {}

    public static Date formDuration(Duration duration) {
        return Date.from(Instant.now().plus(duration));
    }

    public static Date formDuration(Integer time, TimeUnit timeUnit) {
        return Date.from(Instant.now().plus(getDuration(time, timeUnit)));
    }

    public static Date formLocalDateTime(Integer time, TimeUnit timeUnit) {
        return Date.from(getLocalDateTime(time, timeUnit).atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Duration getDuration(Integer time, TimeUnit timeUnit) {
        switch (timeUnit) {
            case DAYS:
                return Duration.ofDays(time);
            case HOURS:
                return Duration.ofHours(time);
            case MINUTES:
                return Duration.ofMinutes(time);
            case SECONDS:
                return Duration.ofSeconds(time);
            case MILLISECONDS:
                return Duration.ofMillis(time);
            case NANOSECONDS:
                return Duration.ofNanos(time);
            default:
                throw new UnsupportedOperationException("Man, use a real TimeUnit unit");
        }
    }

    public static LocalDateTime getLocalDateTime(Integer time, TimeUnit timeUnit) {
        switch (timeUnit) {
            case DAYS:
                return LocalDateTime.now().plusDays(time);
            case HOURS:
                return LocalDateTime.now().plusHours(time);
            case MINUTES:
                return LocalDateTime.now().plusMinutes(time);
            case SECONDS:
                return LocalDateTime.now().plusSeconds(time);
            default:
                throw new UnsupportedOperationException("Man, use a real TimeUnit unit");
        }
    }

    /**
     * 获取年月日[2021, 11, 11]
     *
     * @return
     */
    public static String[] getDateFolder() {
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
     * 替换objectName 首个 /
     *
     * @param objectName
     * @return
     */
    public static String getObjectName(String objectName) {
        return objectName.length() > 1 && objectName.startsWith(MinioConstant.URI_DELIMITER) ? objectName.substring(1) : objectName;
    }

    /**
     * 配置的路径以 / 开始，去掉第一个路径符
     *
     * @param path
     * @return
     */
    public static String getPath(String path) {
        if (path == null || "".equals(path)) {
            return "";
        }
        path = path.startsWith(MinioConstant.URI_DELIMITER) ? path.substring(path.indexOf(MinioConstant.URI_DELIMITER) + 1) : path;
        path = path.endsWith(MinioConstant.URI_DELIMITER) ? path : path + MinioConstant.URI_DELIMITER;
        return path;
    }

    public static HashMultimap<String, String> getHeader(String contentType) {
        if (contentType == null || "".equals(contentType)) {
            contentType = "application/octet-stream";
        }
        HashMultimap<String, String> headers = HashMultimap.create();
        headers.put("Content-Type", contentType);
        return headers;
    }

    public static String getContentType(String contentType) {
        if (contentType == null || "".equals(contentType)) {
            contentType = "application/octet-stream";
        }
        return contentType;
    }

}
