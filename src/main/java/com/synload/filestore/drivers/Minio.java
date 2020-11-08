package com.synload.filestore.drivers;

import com.synload.filestore.utils.StringUtils;
import io.minio.*;
import io.minio.errors.*;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class Minio implements StoreDriver {
    MinioClient minioClient;
    String bucket = "";

    public Minio(String connectionURL, String bucket, String access, String secret) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        this.bucket = bucket;
        this.minioClient = MinioClient.builder()
            .endpoint(connectionURL)
            .credentials(access, secret)
            .build();

        if(!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
            System.out.println("Bucket: "+bucket);
            System.out.println("Access: "+access);
            System.out.println("Secret: "+secret);
            System.out.println("Connection url: "+connectionURL);
            System.out.println("Bucket does not exist!");
            System.exit(1);
        }
    }

    @Override
    public void download(String path, String hash) {
        try {
            minioClient.downloadObject(DownloadObjectArgs.builder().filename(path).object(StringUtils.hashToPath(hash)).bucket(bucket).build());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean exists(String hash) {
        try {
            objectMeta(hash);
            return true;
        } catch (Exception e) {
        }
        return false;
    }

    public StatObjectResponse objectMeta(String hash) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        return minioClient.statObject(StatObjectArgs.builder().bucket(bucket).object(StringUtils.hashToPath(hash)).build());
    }
    @Override
    public long size(String hash) {
        try {
            StatObjectResponse statObjectResponse = objectMeta(hash);
            return statObjectResponse.size();
        } catch (Exception e) {
        }
        return 0;
    }

    @Override
    public void upload(String path, String hash) {
        try {
            String hashPath = StringUtils.hashToPath(hash);
            if(!exists(hash)) {
                minioClient.uploadObject(
                    UploadObjectArgs.builder()
                        .bucket(bucket)
                        .object(hashPath)
                        .filename(path)
                        .build());
                System.out.println("Uploaded: " + hashPath);
            }else{
                //System.out.println("File exists, skipping: " + hashPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
