package allen.example.minio.demo;

import io.minio.*;
import io.minio.errors.MinioException;
import io.minio.http.Method;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.Map;


import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class MinioDemoApplicationTest {

    @Test
    void testCreateObject() {
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("root", "123456789")
                            .build();

            String bucketName = "test";
            String objectName = "nsu3cSp.jpg";
            String filePath = "d:/test/nsu3cSp.jpg";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }

            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .filename(filePath)
                            .build());
            System.out.printf("successfully uploaded as object '%s' to bucket '%s'.%n", objectName, bucketName);
        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testDownloadObject() {
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("root", "123456789")
                            .build();

            String bucketName = "test";
            String objectName = "params.txt";
            String filePath = "d:/test/minio/params.txt";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }
            minioClient.downloadObject(DownloadObjectArgs.builder().bucket(bucketName)
                    .object(objectName).filename(filePath).build());
            System.out.printf("successfully download as object '%s' to bucket '%s'. path -> %s", objectName, bucketName, filePath);
        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testPreSignedDownloadObject(){
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("root", "123456789")
                            .build();

            String bucketName = "test";
            String objectName = "sky.jpg";
            String filePath = "d:/test/minio/params.txt";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }
           String preUrl = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName)
                    .object(objectName).method(Method.PUT).build());
            System.out.printf("successfully getPresignedObjectUrl -> %s", preUrl);
        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testPreSignedUploadObject(){
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("root", "123456789")
                            .build();

            String bucketName = "test";
            String objectName = "sky.jpg";
            String filePath = "E:/resource/sky.jpg";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }
            String preUrl = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName)
                    .object(objectName).method(Method.PUT).build());
            System.out.printf("successfully getPresignedObjectUrl -> %s", preUrl.toString());
            // 3.模拟第三方，使用 OkHttp调用 Post上传对象
            // 创建 MultipartBody对象
            MultipartBody.Builder multipartBuilder = new MultipartBody.Builder();
            multipartBuilder.setType(MultipartBody.FORM);

            // 添加其他必要参数
            multipartBuilder.addFormDataPart("key", objectName);// 必须要和策略参数一样
            multipartBuilder.addFormDataPart("Content-Type", "image/png");

            // 上传文件
            File uploadFile = new File(filePath);

            // 具体参数如下：fun addFormDataPart(name: String, filename: String?, body: RequestBody)
            multipartBuilder.addFormDataPart(
                    "file", objectName, RequestBody.create(uploadFile, null));

            // 使用OkHttp调用Post上传对象
            // 构建 POST 请求
            Request request =
                    new Request.Builder()
                            .url("http://127.0.0.1:9000" + "/" + bucketName)
                            .post(multipartBuilder.build())
                            .build();

            OkHttpClient httpClient = new OkHttpClient().newBuilder().build();
            // 发送 POST 请求
            Response response = httpClient.newCall(request).execute();

            // 检查响应是否成功
            if (response.isSuccessful()) {
                System.out.println("对象上传成功，使用 POST 方法");
            } else {
                System.out.println("对象上传失败");
            }
        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testPreSignedUploadObject2(){
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("root", "123456789")
                            .build();

            String bucketName = "test";
            String objectName = "sky.jpg";
            String filePath = "E:/resource/sky.jpg";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }
            PostPolicy policy = new PostPolicy(bucketName, ZonedDateTime.now().plusDays(1));
            // 设置一个参数key-value，值为上传对象的名称（保存在桶中的名字）
            policy.addEqualsCondition("key", objectName);
            // 设置文件的格式，public void addStartsWithCondition(@Nonnull String element, @Nonnull String value)
            // 添加 Content-Type以"image/"开头，表示只能上传照片
            policy.addStartsWithCondition("Content-Type", "image/");
            Map<String, String> formData = minioClient.getPresignedPostFormData(policy);
            System.out.printf("successfully getPresignedPostFormData -> %s", formData.toString());
            // 3.模拟第三方，使用 OkHttp调用 Post上传对象
            // 创建 MultipartBody对象
            MultipartBody.Builder multipartBuilder = new MultipartBody.Builder();
            multipartBuilder.setType(MultipartBody.FORM);

            // 将认证令牌和签名等信息添加到请求体中
            for (Map.Entry<String, String> entry : formData.entrySet()) {
                multipartBuilder.addFormDataPart(entry.getKey(), entry.getValue());
            }

            // 添加其他必要参数
            multipartBuilder.addFormDataPart("key", objectName);// 必须要和策略参数一样
            multipartBuilder.addFormDataPart("Content-Type", "image/png");

            // 上传文件
            File uploadFile = new File(filePath);

            // 具体参数如下：fun addFormDataPart(name: String, filename: String?, body: RequestBody)
            multipartBuilder.addFormDataPart(
                    "file", objectName, RequestBody.create(uploadFile, null));

            // 使用OkHttp调用Post上传对象
            // 构建 POST 请求
            Request request =
                    new Request.Builder()
                            .url("http://127.0.0.1:9000" + "/" + bucketName)
                            .post(multipartBuilder.build())
                            .build();

            OkHttpClient httpClient = new OkHttpClient().newBuilder().build();
            // 发送 POST 请求
            Response response = httpClient.newCall(request).execute();

            // 检查响应是否成功
            if (response.isSuccessful()) {
                System.out.println("对象上传成功，使用 POST 方法");
            } else {
                System.out.println("对象上传失败");
            }
        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

//    @Test
//    void testDeleteObject(){
//
//    }
//
}