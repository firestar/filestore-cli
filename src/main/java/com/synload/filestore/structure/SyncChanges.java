package com.synload.filestore.structure;

public class SyncChanges {
    long offsetFull;
    long offsetChanges;
    Changes changes;

    public SyncChanges() {
    }

    public SyncChanges(long offsetFull, long offsetChanges, Changes changes) {
        this.offsetFull = offsetFull;
        this.offsetChanges = offsetChanges;
        this.changes = changes;
    }

    public Changes getChanges() {
        return changes;
    }

    public void setChanges(Changes changes) {
        this.changes = changes;
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
