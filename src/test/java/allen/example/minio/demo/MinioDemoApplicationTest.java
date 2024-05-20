package allen.example.minio.demo;

import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class MinioDemoApplicationTest {

    @Test
    void testCreateObject() {
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("admin", "password")
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
                            .credentials("admin", "password")
                            .build();

            String bucketName = "test";
            String objectName = "nsu3cSp.jpg";
            String filePath = "d:/test/tmp/nsu3cSp.jpg";
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

    //    @Test
//    void testDeleteObject(){
//
//    }
//
    @Test
    void testPreSignedUploadObject() {
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("admin", "password")
                            .build();

            String bucketName = "test";
            String objectName = "upload2.jpg";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }
            String preSignedURL = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName).method(Method.PUT)
                    .object(objectName).expiry(24 * 60 * 60).build());
            System.out.printf("successfully download as object '%s' to bucket '%s'. preSignedURL -> %s", objectName, bucketName, preSignedURL);
        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }
//
//    @Test
//    void testPreSignedDownloadObject(){
//
//    }

    @Test
    void testMultipleUpload() throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint("http://127.0.0.1:9000")
                        .credentials("admin", "password")
                        .build();


        String bucketName = "test";
        // Create new post policy for 'my-bucketname' with 7 days expiry from now.
        PostPolicy policy = new PostPolicy(bucketName, ZonedDateTime.now().plusDays(7));

        // Add condition that 'key' (object name) equals to 'my-objectname'.
        policy.addEqualsCondition("key", "my-objectname");

        // Add condition that 'Content-Type' starts with 'image/'.
        policy.addStartsWithCondition("Content-Type", "image/");

        // Add condition that 'content-length-range' is between 64kiB to 10MiB.
        policy.addContentLengthRangeCondition(64 * 1024, 10 * 1024 * 1024);

        Map<String, String> formData = minioClient.getPresignedPostFormData(policy);
        System.out.println(formData);

//        // Upload an image using POST object with form-data.
//        MultipartBody. Builder multipartBuilder = new MultipartBody. Builder();
//        multipartBuilder. setType(MultipartBody. FORM);
//        for (Map. Entry<String, String> entry : formData. entrySet()) {
//            multipartBuilder. addFormDataPart(entry. getKey(), entry. getValue());
//        }
//        multipartBuilder. addFormDataPart("key", "my-objectname");
//        multipartBuilder. addFormDataPart("Content-Type", "image/ png");
//
//        // "file" must be added at last.
//        multipartBuilder. addFormDataPart(
//                "file", "my-objectname", RequestBody. create(new File("Pictures/ avatar. png"), null));
//
//        Request request =
//                new Request. Builder()
//                        .url("https:// play. min. io/ my-bucketname")
//                        .post(multipartBuilder. build())
//                        .build();
//        OkHttpClient httpClient = new OkHttpClient().newBuilder().build();
//        Response response = httpClient. newCall(request).execute();
//        if (response. isSuccessful()) {
//            System. out. println("Pictures/ avatar. png is uploaded successfully using POST object");
//        } else {
//            System. out. println("Failed to upload Pictures/ avatar. png");
//        }
    }

    /**
     * split a file in local. Like that:
     * $ split -b 6344k video_20230923_122646.mp4
     */
    @Test
    void testMultipleChunkUpload() {


        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://127.0.0.1:9000")
                            .credentials("admin", "password")
                            .build();

            String bucketName = "test";
            // Make 'test' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                // Make a new bucket called 'test'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                System.out.printf("Bucket %s already exists.%n", bucketName);
            }
            Arrays.stream(new String[]{"xaaa", "xaab", "xaac"}).forEach(fileName -> {
                String preSignedURL = null;
                try {
                    preSignedURL = minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName).method(Method.PUT)
                            .object(fileName).expiry(24 * 60 * 60).build());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println(String.format("successfully download as object '%s' to bucket '%s'. preSignedURL -> %s", fileName, bucketName, preSignedURL));

            });

        } catch (MinioException e) {
            log.error("{} {}", e.getMessage(), e.httpTrace(), e);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    @Test
    void testComposeChunks() {
        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint("http://127.0.0.1:9000")
                        .credentials("admin", "password")
                        .build();

        String bucketName = "test";
        String composedObjectName = "composed" + UUID.randomUUID().toString() + ".mp4";
        // Make 'test' bucket if not exist.
        boolean found =
                minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        if (!found) {
            // Make a new bucket called 'test'.
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } else {
            System.out.printf("Bucket %s already exists.%n", bucketName);
        }
        ObjectWriteResponse response = minioClient.composeObject(
                ComposeObjectArgs.builder()
                        .bucket(bucketName)
                        .object(composedObjectName)
                        .sources(Arrays.stream(new String[]{"xaaa", "xaab", "xaac"})
                                .map(oName -> ComposeSource.builder().bucket(bucketName).object(oName).build())
                                .collect(Collectors.toList())).build());
        System.out.println(response.headers());
        System.out.println(String.format("successfully composeObject as object '%s' to bucket '%s'. ", composedObjectName, bucketName));
    }

    /**
     * It is md5sum(md5-part1.. md5-partN)-N
     * https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums
     */
    @SneakyThrows
    @Test
    void testMd5() {
//        byte[] array1 = "6fe501c6dd09c693e4b6b7328bf87399".getBytes("utf-8");
//        byte[] array2 = "a4fe1afc0a8a217cc67c9bdf709e6465".getBytes("utf-8");
//        byte[] array3 = "541a58907e488d452d21f68520594094".getBytes("utf-8");
//
//        byte[] result = new byte[array1.length + array2.length + array3.length];
//
//        // 复制第一个数组到结果数组
//        System.arraycopy(array1, 0, result, 0, array1.length);
//
//        // 复制第二个数组到结果数组，注意起始位置是第一个数组的长度
//        System.arraycopy(array2, 0, result, array1.length, array2.length);
//
//        // 复制第三个数组到结果数组，起始位置是前两个数组的总长度
//        System.arraycopy(array3, 0, result, array1.length + array2.length, array3.length);
//        System.out.println(DigestUtils.md5Hex(result));
//
        System.out.println(getMD5Hash("6fe501c6dd09c693e4b6b7328bf87399"+"a4fe1afc0a8a217cc67c9bdf709e6465"+"541a58907e488d452d21f68520594094"));
////        System.out.println(getMD5Hash("541a58907e488d452d21f68520594094"+"a4fe1afc0a8a217cc67c9bdf709e6465"+"6fe501c6dd09c693e4b6b7328bf87399"));
////        System.out.println(calculateMD5("D:\\test\\xaa"));
//
//        String checksum = DigestUtils.md5Hex(Files.newInputStream(Paths.get("D:\\\\test\\\\xaa")));
//        System.out.println(checksum);
//
//        checksum = DigestUtils.md5Hex("6fe501c6dd09c693e4b6b7328bf87399"+"a4fe1afc0a8a217cc67c9bdf709e6465"+"541a58907e488d452d21f68520594094");
//        System.out.println(checksum);

        String[] segmentMD5s = {"6fe501c6dd09c693e4b6b7328bf87399", "a4fe1afc0a8a217cc67c9bdf709e6465", "541a58907e488d452d21f68520594094"};

        // 计算分段的ETag
        String segmentedETag = computeSegmentedETag(segmentMD5s);

        System.out.println(segmentedETag);
    }

    public static String computeSegmentedETag(String[] segmentMD5s) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");

        // 对分段的MD5进行排序
//        Arrays.sort(segmentMD5s);

        // 将排序后的分段MD5连接起来
        StringBuilder etagBuilder = new StringBuilder();
        for (String segmentMD5 : segmentMD5s) {
            etagBuilder.append(segmentMD5);
        }
        String etagString = etagBuilder.toString();

        // 计算连接后的ETag字符串的MD5
        byte[] etagBytes = etagString.getBytes(StandardCharsets.UTF_8);
        md.update(etagBytes);
        byte[] digest = md.digest();

        // 将MD5转换为十六进制字符串
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }


    public static String getMD5Hash(String input) {
        try {
            // 获取MD5摘要算法的 MessageDigest 对象
            MessageDigest md = MessageDigest.getInstance("MD5");

            // 使用指定的字节更新摘要信息
            md.update(input.getBytes());

            // 得到密文（即：MD5校验和）
            byte[] digest = md.digest();

            // 将二进制转换成16进制字符串形式
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }

            // 返回MD5的16进制字符串
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5算法未找到", e);
        }
    }

    public static String calculateMD5(String filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
        }
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}