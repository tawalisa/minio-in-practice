package allen.example.minio.demo;

import io.minio.*;
import io.minio.errors.MinioException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;


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
//    @Test
//    void testPreSignedUploadObject(){
//
//    }
//
//    @Test
//    void testPreSignedDownloadObject(){
//
//    }

}