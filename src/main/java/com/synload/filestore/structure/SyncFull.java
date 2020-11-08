package com.synload.filestore.structure;

public class SyncFull {
    long offsetFull;
    long offsetChanges;
    Folder folder;

    public SyncFull() {
    }

    public SyncFull(long offsetFull, long offsetChanges, Folder folder) {
        this.offsetFull = offsetFull;
        this.offsetChanges = offsetChanges;
        this.folder = folder;
    }

    public Folder getFolder() {
        return folder;
    }

    public void setFolder(Folder folder) {
        this.folder = folder;
    }

    public long getOffsetFull() {
        return offsetFull;
    }

    public void setOffsetFull(long offsetFull) {
        this.offsetFull = offsetFull;
    }

    public long getOffsetChanges() {
        return offsetChanges;
    }

    public void setOffsetChanges(long offsetChanges) {
        this.offsetChanges = offsetChanges;
    }
}
