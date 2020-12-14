package com.synload.filestore.storagedrivers;

import com.synload.filestore.structure.Folder;

public class Local implements StoreDriver {
    @Override
    public void download(String path, String hash) {

    }

    @Override
    public boolean exists(String hash) {
        return false;
    }

    @Override
    public long size(String hash) {
        return 0;
    }

    @Override
    public void upload(String path, String hash) {

    }

    @Override
    public Folder folder(String path) {
        return null;
    }
}
