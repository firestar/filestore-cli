package com.synload.filestore.storagedrivers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.synload.filestore.structure.Folder;
import com.synload.filestore.utils.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URI;

public class S3Bucket implements StoreDriver {
    String bucket;
    AmazonS3 s3client;

    public S3Bucket(String region, String bucket, String access, String secret) {
        this.bucket = bucket;
        AWSCredentials credentials = new BasicAWSCredentials(
            access,
            secret
        );
        this.s3client = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.fromName(region))
            .build();
        if(s3client.doesBucketExistV2(bucket)) {
            System.out.println("Bucket does not exist!");
            System.exit(1);
        }
    }

    @Override
    public void download(String path, String hash) {
        String hashPath = StringUtils.hashToPath(hash);
        if(!exists(hash)) {
            try{
                s3client.getObject(
                    new GetObjectRequest(
                        bucket,
                        hashPath
                    ),
                    new File(path)
                );
                System.out.println("Download Complete: " + hashPath);
            } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
                    + "to Amazon S3, but was rejected with an error response" + " for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());

            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which " + "means the client encountered " + "an internal error while trying to "
                    + "communicate with S3, " + "such as not being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());

            }
        }else{
            System.out.println("File exists, skipping: " + hashPath);
        }
    }

    @Override
    public boolean exists(String hash) {
        System.out.println("Checking if file exists: "+ StringUtils.hashToPath(hash));
        ObjectMetadata objectMetadata = null;
        try {
            objectMetadata = s3client.getObjectMetadata(new GetObjectMetadataRequest(bucket, StringUtils.hashToPath(hash)));
        }catch (Exception e){

        }
        return objectMetadata!=null;
    }

    @Override
    public long size(String hash) {
        return s3client.getObjectMetadata(new GetObjectMetadataRequest(bucket, StringUtils.hashToPath(hash))).getContentLength();
    }

    @Override
    public void upload(String path, String hash) {
        String hashPath = StringUtils.hashToPath(hash);
        if(!exists(hash)) {
            try{
                s3client.putObject(
                    new PutObjectRequest(
                        bucket,
                        hashPath,
                        new File(path)
                    )
                );
                System.out.println("Upload Complete: " + hashPath);
            } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
                    + "to Amazon S3, but was rejected with an error response" + " for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());

            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which " + "means the client encountered " + "an internal error while trying to "
                    + "communicate with S3, " + "such as not being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());

            }
        }else{
            System.out.println("File exists, skipping: " + hashPath);
        }
    }

    @Override
    public Folder folder(String path) {
        return null;
    }
}
