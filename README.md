## minio-spring-boot-starter

### maven

[![Maven Central](https://img.shields.io/maven-central/v/com.kangaroohy/minio-spring-boot-starter.svg)](https://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.kangaroohy%22%20AND%20a%3A%minio-spring-boot-starter%22)

~~~xml
<dependency>
  <groupId>com.kangaroohy</groupId>
  <artifactId>minio-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
~~~

### 1 封装方法
![method.png](method.png)

### 2 使用

```yaml
kangaroohy:
  minio:
    endpoint: http://192.168.68.128:9528
    access-key: admin
    secret-key: admin123
```
配置完成，调用封装好的方法即可

其中，getPolicy() 方法用于获取前端 post formData 直传的相关凭证信息，如下：

后端接口：
```java
    @GetMapping("/oss/policy")
    public RestResult<Map<String, String>> policy(@RequestParam String fileName) throws MinioException {
        Map<String, String> policy = minioService.getPolicy(bucketName, fileName);
        return RestResult.ok(policy);
    }
```
前端调用：
```javascript
httpRequestHandle(param) {
    let _this = this;
    let file = param.file;
    let data = {
        fileName: file.name
    }
    getRequest("/oss/policy", data).then(resp => {
        let {xamzalgorithm, xamzcredential, policy, xamzsignature, xamzdate, host, key} = resp.data;
        let formData = new FormData();
        formData.append("key", key);
        formData.append("x-amz-algorithm", xamzalgorithm);
        formData.append("x-amz-credential", xamzcredential);
        formData.append("policy", policy);
        formData.append("x-amz-signature", xamzsignature);
        formData.append("x-amz-date", xamzdate);
        formData.append("file", file);
        httpRequest("post", host, formData).then(res => {
            if (res.status === 204) {
                _this.directUrl = res.headers.location;
            }
        })
    })
}
```

getPolicyUrl() 方法用于获取前端 put 直传的 url 信息，如下：

后端接口：
```java
    @GetMapping("/oss/uploadUrl")
    public RestResult<String> uploadUrl(@RequestParam String fileName) throws MinioException {
        String url = minioService.getPolicyUrl(bucketName, fileName);
        return RestResult.ok(url);
    }
```
前端调用：
```javascript
urlUploadHandle(param) {
    let _this = this;
    let file = param.file;
    let data = {
        fileName: file.name
    }
    getRequest("/oss/uploadUrl", data).then(resp => {
        let url = resp.data;
        // 发送 put 请求
        let config = {'Content-Type': file.type}
        httpRequest("put", url, file, config).then(res => {
            if (res.status == 200) {
                _this.uploadUrl = url.substring(0, url.indexOf("?"))
            }
        })
    });
}
```

经过测试，getPolicyUrl() 方式传输效率 优于 getPolicy() 方式

这两种方式上传文件，后端充当凭证获取的角色，上传文件不经过后端，由前端直传

其余方法，按需调用

如果封装的方法不满足需求，可以注入 `MinioClient minioClient` 调用 minioClient 提供的原始方法进行操作

```java
    @Autowired
    private MinioClient minioClient;
```