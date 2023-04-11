## 3.0.8

- 升级minio到最新版本 8.5.2
- 部分代码优化

## 3.0.6

- 升级minio到最新版本 8.5.1

## 3.0.4

- 前端直传两个方法名优化

![img.png](img.png)

## 3.0.1

3.x 版本适配 spring boot 3.x，也可在spring boot 2.x中使用

- minio sdk版本升级到目前官方最新版 8.4.6
- 分片上传api调整以适配最新版sdk
- 修复分片上传无法合并成原文件的bug

## 1.0.1

- 默认内置三种bucket访问策略（只读，可读写，只写），新增设置桶策略方法 `setBucketPolicy`
- 支持创建bucket时指定bucket访问策略

## 1.0.0

- 首次封装，提供以下方法

 ![method.png](method.png)