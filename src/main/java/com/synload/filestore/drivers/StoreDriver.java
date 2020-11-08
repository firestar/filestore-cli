package com.synload.filestore.drivers;

import io.minio.errors.*;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public interface StoreDriver {
    void download(String path, String hash);
    boolean exists(String hash);
    long size(String hash);
    void upload(String path, String hash);
}
