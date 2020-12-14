package com.synload.filestore.storagedrivers;

import com.synload.filestore.structure.Folder;

public interface StoreDriver {
    void download(String path, String hash);
    boolean exists(String hash);
    long size(String hash);
    void upload(String path, String hash);
    Folder folder(String path);
}
