package com.synload.filestore.kafka;

import com.synload.filestore.structure.Changes;
import com.synload.filestore.structure.SyncChanges;
import com.synload.filestore.structure.SyncFull;

public interface ChangeDetection {
    void change(SyncChanges changes);
}
