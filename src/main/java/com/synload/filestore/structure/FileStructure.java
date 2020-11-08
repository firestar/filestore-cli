package com.synload.filestore.structure;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileStructure {
    public static Folder generate(String path) throws IOException {
        return new Folder(new File(path));
    }
    public static SyncFull openLastSync(String path) throws IOException {
        if(!exists(path))
            return null;
        return new ObjectMapper().readValue(Paths.get(path,".store").toFile(), SyncFull.class);
    }
    public static void save(String path, SyncFull syncFull) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(path,".store").toFile()));
        writer.write(new ObjectMapper().writeValueAsString(syncFull));
        writer.close();
    }
    public static boolean exists(String path) throws IOException {
        return Paths.get(path,".store").toFile().exists();
    }
    public static void displayChanges(Changes changes){
        System.out.println("Added: ");
        System.out.println(collapseFolder("\t", changes.getAdditions()).stream().map(file->(String)file[0]).collect(Collectors.joining("\n")));
        System.out.println("Deleted: ");
        System.out.println(collapseFolderDeletions("\t", changes.getDeletions()).stream().collect(Collectors.joining("\n")));
        System.out.println("Modified: ");
        System.out.println(collapseFolder("\t", changes.getModifications()).stream().map(file->(String)file[0]).collect(Collectors.joining("\n")));
    }
    public static void displayFolder(Folder folder){
        System.out.println("Files: ");
        System.out.println(collapseFolder("\t", folder).stream().map(file->(String)file[0]).collect(Collectors.joining("\n")));
    }
    public static void deleteFolder(File file){
        for (File listFile : file.listFiles()) {
            if(listFile.isDirectory()){
                deleteFolder(listFile);
            }else{
                listFile.delete();
            }
        }
        file.delete();
    }
    public static void delete(String path, Folder folder){
        folder.getFolders().entrySet().stream().forEach(c->{
            if(c.getValue()==null) {
                deleteFolder(new File(path + File.separator + c.getKey()));
            }else
                delete(path+File.separator+c.getKey(), c.getValue());
        });
        folder.getFiles().entrySet().stream().forEach(c->{
            new File(path+File.separator+c.getKey()).delete();
        });
    }
    public static void mkdir(String path, Folder folder){
        folder.getFolders().entrySet().stream().forEach(c->{
            new File(path+File.separator+c.getKey()).mkdir();
            mkdir(path+File.separator+c.getKey(), c.getValue());
        });
    }
    public static List<String> collapseFolderDeletions(String path, Folder folder){
        List<String> files = new ArrayList<>();
        if(folder!=null) {
            folder.getFolders().entrySet().stream().forEach(c -> {
                if(c.getValue()==null){
                    files.add(path + c.getKey() + File.separator);
                }
                files.addAll(collapseFolderDeletions(path + c.getKey() + File.separator, c.getValue()));
            });
            folder.getFiles().entrySet().stream().forEach(c -> {
                if(c.getValue()==null) {
                    files.add(path + c.getKey());
                }
            });
        }
        return files;
    }
    public static Changes localChanges(String path, Folder folder){
        Changes changes = new Changes(
            folder.localAdditions(new File(path)),
            folder.localDeletions(new File(path)),
            folder.localModifications(new File(path))
        );
        return changes;
    }
    public static Changes remoteChanges(Folder base, Folder folder){
        Changes changes = new Changes(
            base.remoteAdditions(folder),
            base.remoteDeletions(folder),
            base.remoteModifications(folder)
        );
        return changes;
    }
    public static Changes combineChanges(List<Changes> changesList){
        return null;
    }
    public static void applyChange(Folder folder, Changes changes){
        folder.add(changes.getAdditions());
        folder.delete(changes.getDeletions());
        folder.modify(changes.getModifications());
    }
    public static void applyChanges(Folder folder, List<Changes> changesList){
        changesList.stream().forEach(c->{
            folder.add(c.getAdditions());
            folder.delete(c.getDeletions());
            folder.modify(c.getModifications());
        });
    }
    public static List<Object[]> collapseFolder(String path, Folder folder){
        List<Object[]> files = new ArrayList<>();
        if(folder!=null) {
            folder.getFolders().entrySet().stream().forEach(c -> {
                files.addAll(collapseFolder(path +File.separator+ c.getKey(), c.getValue()));
            });
            folder.getFiles().entrySet().stream().forEach(c -> files.add(new Object[]{path + File.separator + c.getKey(), c.getValue().getHash(), c.getValue().getModified()}));
        }
        return files;
    }
    public static List<Object[]> collapseChanges(String path, Changes changes){
        List<Object[]> files = new ArrayList<>();
        if(changes.getAdditions()!=null) {
            changes.getAdditions().getFolders().entrySet().stream().forEach(c -> {
                files.addAll(collapseFolder(path +File.separator+ c.getKey(), c.getValue()));
            });
            changes.getAdditions().getFiles().entrySet().stream().forEach(c -> files.add(new Object[]{path + File.separator + c.getKey(), c.getValue().getHash(), c.getValue().getModified()}));
        }
        if(changes.getModifications()!=null) {
            changes.getModifications().getFolders().entrySet().stream().forEach(c -> {
                files.addAll(collapseFolder(path +File.separator+ c.getKey(), c.getValue()));
            });
            changes.getModifications().getFiles().entrySet().stream().forEach(c -> files.add(new Object[]{path + File.separator + c.getKey(), c.getValue().getHash(), c.getValue().getModified()}));
        }
        return files;
    }
}
