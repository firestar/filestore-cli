package com.synload.filestore.structure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class Folder {
    Map<String, File> files = new HashMap<>();
    Map<String, Folder> folders = new HashMap<>();

    public Folder() {

    }

    public Folder(Folder other) {
        other.files.entrySet().stream().forEach(c->files.put(c.getKey(), new File(c.getValue())));
        other.folders.entrySet().stream().forEach(c->folders.put(c.getKey(),new Folder(c.getValue())));
    }

    public Folder(java.io.File folder) throws IOException {
        if (folder.isDirectory() && folder.exists()) {
            for (java.io.File listFile : folder.listFiles()) {
                String filename = listFile.getName();
                if (filename.equals(".store") || filename.equals(".") || filename.equals("..") || !listFile.exists()) {
                    continue;
                }
                if (listFile.isDirectory()) {
                    if (!Files.isSymbolicLink(listFile.toPath())) {
                        folders.put(filename, new Folder(listFile));
                    }
                } else {
                    try {
                        File file = new File(listFile);
                        files.put(filename, file);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public Folder localAdditions(java.io.File folder) {
        Set<String> innerFolders = this.folders.keySet();
        Set<String> innerFiles = this.files.keySet();
        Folder wrapper = new Folder();
        if (folder.isDirectory() && folder.exists()) {
            for (java.io.File listFile : folder.listFiles()) {
                String filename = listFile.getName();
                if (filename.equals(".store") || filename.equals(".") || filename.equals("..") || !listFile.exists()) {
                    continue;
                }
                if (listFile.isDirectory()) {
                    if (!innerFolders.contains(filename)) {
                        if (!Files.isSymbolicLink(listFile.toPath())) {
                            try {
                                wrapper.folders.put(filename, new Folder(listFile));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    } else {
                        Folder folObject = folders.get(filename);
                        folObject = folObject.localAdditions(listFile);
                        if (folObject.files.size() > 0 || folObject.folders.size() > 0) {
                            wrapper.folders.put(filename, folObject);
                        }
                    }
                } else {
                    if (!innerFiles.contains(filename)) {
                        try {
                            File file = new File(listFile);
                            wrapper.files.put(filename, file);
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return wrapper;
    }

    public Folder localDeletions(java.io.File folder) {
        Set<String> innerFolders = new HashSet<>(this.folders.keySet());
        Set<String> innerFiles = new HashSet<>(this.files.keySet());
        Folder wrapper = new Folder();
        if (folder.isDirectory() && folder.exists()) {
            for (java.io.File listFile : folder.listFiles()) {
                String filename = listFile.getName();
                if (filename.equals(".store") || filename.equals(".") || filename.equals("..") || !listFile.exists()) {
                    continue;
                }
                if (listFile.isDirectory()) {
                    if (innerFolders.contains(filename)) {
                        Folder folderObj = this.folders.get(filename).localDeletions(listFile);
                        if (folderObj.folders.size() > 0 || folderObj.files.size() > 0) {
                            wrapper.folders.put(filename, folderObj);
                        }
                        innerFolders.remove(filename);
                    }
                } else {
                    if (innerFiles.contains(filename)) {
                        innerFiles.remove(filename);
                    }
                }
            }
            innerFiles.forEach(x -> wrapper.files.put(x, null));
            innerFolders.forEach(x -> wrapper.folders.put(x, null));
        }
        return wrapper;
    }

    public Folder localModifications(java.io.File folder) {
        Folder wrapper = new Folder();
        if (folder.isDirectory() && folder.exists()) {
            for (java.io.File listFile : folder.listFiles()) {
                String filename = listFile.getName();
                if (filename.equals(".store") || filename.equals(".") || filename.equals("..") || !listFile.exists()) {
                    continue;
                }
                if (listFile.isDirectory()) {
                    if (folders.containsKey(filename)) {
                        Folder folObject = folders.get(filename);
                        folObject = folObject.localModifications(listFile);
                        if (folObject.files.size() > 0 || folObject.folders.size() > 0) {
                            wrapper.folders.put(filename, folObject);
                        }
                    }
                } else {
                    if (files.containsKey(filename)) {
                        if (listFile.lastModified() != files.get(filename).modified) {
                            try {
                                wrapper.files.put(filename, new File(listFile));
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
        return wrapper;
    }

    public Folder remoteModifications(Folder folder) {
        Folder wrapper = new Folder();
        folder.getFolders().entrySet().stream().forEach(c->{
            String folderName = c.getKey();
            if(folders.containsKey(folderName)) {
                Folder wrapperInner = folders.get(folderName).remoteModifications(c.getValue());
                if(wrapperInner.getFolders().size()>0 || wrapperInner.getFiles().size()>0){
                    wrapper.folders.put(c.getKey(), wrapperInner);
                }
            }
        });
        folder.getFiles().entrySet().stream().forEach(c->{
            String fileName = c.getKey();
            if (files.containsKey(fileName) && (!c.getValue().getHash().equals(files.get(fileName).getHash()) || !c.getValue().getModified().equals(files.get(fileName).getModified()))) {
                wrapper.files.put(fileName, new File(c.getValue()));
            }
        });
        return wrapper;
    }
    public Folder remoteDeletions(Folder folder) {
        Set<String> innerFolders = new HashSet<>(this.folders.keySet());
        Set<String> innerFiles = new HashSet<>(this.files.keySet());
        Folder wrapper = new Folder();
        folder.folders.entrySet().stream().forEach(c->{
            String folderName = c.getKey();
            if(innerFolders.contains(folderName)) {
                Folder folderObj = this.folders.get(folderName).remoteDeletions(c.getValue());
                if (folderObj.folders.size() > 0 || folderObj.files.size() > 0) {
                    wrapper.folders.put(folderName, folderObj);
                }
                innerFolders.remove(folderName);
            }
        });
        folder.files.entrySet().stream().forEach(c->{
            String fileName = c.getKey();
            if (innerFiles.contains(fileName)) {
                innerFiles.remove(fileName);
            }
        });
        innerFiles.forEach(x -> wrapper.files.put(x, null));
        innerFolders.forEach(x -> wrapper.folders.put(x, null));
        return wrapper;
    }

    public Folder remoteAdditions(Folder folder) {
        Folder wrapper = new Folder();
        folder.getFolders().entrySet().stream().forEach(c->{
            String folderName = c.getKey();
            if(!folders.containsKey(folderName)) {
                wrapper.folders.put(folderName, new Folder(c.getValue()));
            }else {
                Folder wrapperInner = folders.get(folderName).remoteAdditions(c.getValue());
                if (wrapperInner.files.size() > 0 || wrapperInner.folders.size() > 0) {
                    wrapper.folders.put(folderName, wrapperInner);
                }

            }
        });
        folder.getFiles().entrySet().stream().forEach(c->{
            String fileName = c.getKey();
            if(!files.containsKey(fileName)) {
                wrapper.files.put(fileName, new File(c.getValue()));
            }
        });
        return wrapper;
    }


    public Map<String, File> getFiles() {
        return files;
    }

    public void setFiles(Map<String, File> files) {
        this.files = files;
    }

    public Map<String, Folder> getFolders() {
        return folders;
    }

    public void setFolders(Map<String, Folder> folders) {
        this.folders = folders;
    }

    public void delete(Folder folder){
        folder.getFolders().entrySet().stream().filter(c->c.getValue()==null).forEach(c->folders.remove(c.getKey()));
        folder.getFiles().entrySet().stream().forEach(c->files.remove(c.getKey()));
        folder.getFolders().entrySet().stream().filter(c->c.getValue()!=null).forEach(c->{
            if(folders.containsKey(c.getKey())){
                folders.get(c.getKey()).delete(c.getValue());
            }
        });
    }
    public void add(Folder folder){
        folder.getFiles().entrySet().stream().forEach(c->files.put(c.getKey(), new File(c.getValue())));
        folder.getFolders().entrySet().stream().forEach(c->{
            if(!folders.containsKey(c.getKey())){
                folders.put(c.getKey(), new Folder());
            }
            folders.get(c.getKey()).add(c.getValue());
        });
    }
    public void modify(Folder folder){
        folder.getFiles().entrySet().stream().forEach(c->files.put(c.getKey(), new File(c.getValue())));
        folder.getFolders().entrySet().stream().forEach(c->{
            if(folders.containsKey(c.getKey())){
                folders.get(c.getKey()).modify(c.getValue());
            }
        });
    }
    public boolean equals(Folder folder){
        if(folder.getFolders().entrySet().stream().map(c->{
            if(folders.containsKey(c.getKey()))
                return c.getValue().equals(folders.get(c.getKey()));
            return false;
        }).filter(c->!c).count()!=0){
            return false;
        }
        if(folder.getFiles().entrySet().stream().map(c->{
            if(files.containsKey(c.getKey())){
                File f = files.get(c.getKey());
                return f.getHash().equals(c.getValue().getHash()) && f.getModified().equals(c.getValue());
            }
            return false;
        }).filter(c->!c).count()!=0){
            return false;
        }
        return true;
    }
}
