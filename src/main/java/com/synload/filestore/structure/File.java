package com.synload.filestore.structure;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class File {
    String hash;
    Long modified;

    public File() {
    }

    public File(File other) {
        this.hash = other.hash;
        this.modified = other.modified;
    }

    public File(java.io.File file) throws FileNotFoundException {
        this.modified = file.lastModified();
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
            this.hash = DigestUtils.sha256Hex(fileInputStream);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fileInputStream!=null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public Long getModified() {
        return modified;
    }

    public void setModified(Long modified) {
        this.modified = modified;
    }
}
