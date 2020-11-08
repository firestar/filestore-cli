package com.synload.filestore.structure;

import com.synload.filestore.structure.Folder;

import java.util.HashMap;
import java.util.Map;

public class Changes {
    Folder deletions;
    Folder additions;
    Folder modifications;
    Map<String, String> meta = new HashMap<>();

    public Changes() {
    }

    public Changes(Folder additions, Folder deletions, Folder modifications) {
        this.deletions = deletions;
        this.additions = additions;
        this.modifications = modifications;
    }

    public Folder getDeletions() {
        return deletions;
    }

    public void setDeletions(Folder deletions) {
        this.deletions = deletions;
    }

    public Folder getAdditions() {
        return additions;
    }

    public void setAdditions(Folder additions) {
        this.additions = additions;
    }

    public Folder getModifications() {
        return modifications;
    }

    public void setModifications(Folder modifications) {
        this.modifications = modifications;
    }

    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }
    public boolean hasChanges(){
        return additions.getFiles().size()>0 || additions.getFolders().size()>0 || deletions.getFiles().size()>0 || deletions.getFolders().size()>0 || modifications.getFiles().size()>0 || modifications.getFolders().size()>0;
    }
}
