package com.synload.filestore;

import com.synload.filestore.drivers.StoreDriver;
import com.synload.filestore.drivers.S3Bucket;
import com.synload.filestore.drivers.Minio;
import com.synload.filestore.kafka.Client;
import com.synload.filestore.kafka.Consumer;
import com.synload.filestore.structure.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class FileStoreCLI {
    private static Logger logger = LoggerFactory.getLogger(FileStoreCLI.class);
    public static Options generateOptions(){
        Options options = new Options();
        Option option = new Option("p", "path", true, "Path to root of files to store in topic [/path/]");
        option.setRequired(true);
        options.addOption(option);
        option = new Option("o", "option", true, "Options:" +
            "\n\tchanges/c\t\t - Show all changes." +
            "\n\texport/e\t\t - Generate checksums and send to file store" +
            "\n\tgrab/g\t\t - Download files from file store" +
            "\n\tpreview/p\t\t - Preview grab");
        option.setRequired(true);
        options.addOption(option);
        Option o = new Option("k", "kafka", true, "Kafka server");
        o.setRequired(true);
        options.addOption(o);
        options.addOption(new Option("g", "group", true, "Kafka group name"));
        option = new Option("t", "topic", true, "Kafka topic containing filesystem state");
        option.setRequired(true);
        options.addOption(option);
        options.addOption(new Option("d", "destination", true, "Storage destination [Minio, AWS]"));
        options.addOption(new Option("b", "bucket", true, "Bucket name"));
        options.addOption(new Option("r", "region", true, "Region name [US_WEST_1]"));
        options.addOption(new Option("a", "access", true, "Access Key"));
        options.addOption(new Option("s", "secret", true, "Secret Key"));
        options.addOption(new Option("u", "connectionURI", true, "Connection URI [http://localhost:9000/] {Use with Minio/FTP}"));
        options.addOption(new Option("e", "execute", true, "Execute shell script"));
        return options;
    }
    public static void main(String[] args){
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        Client kafkaClient = null;
        StoreDriver fileStore = null;
        try {
            cmd = parser.parse(generateOptions(), args);
            if(cmd.hasOption("d")){
                switch(cmd.getOptionValue("d")){
                    case "Minio":
                        List<String> required = Arrays.asList("access", "secret", "connectionURI", "bucket");
                        if(required.stream().filter(c->cmd.hasOption(c)).count()==4) {
                            try {
                                fileStore = new Minio(cmd.getOptionValue("u"), cmd.getOptionValue("b"), cmd.getOptionValue("a"), cmd.getOptionValue("s"));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }else{
                            logger.debug("Missing "+required.stream().filter(c->!cmd.hasOption(c)).collect(Collectors.joining(", ")));
                            System.exit(1);
                        }
                        break;
                    case "AWS":
                        required = Arrays.asList("access", "secret", "region", "bucket");
                        if(required.stream().filter(c->cmd.hasOption(c)).count()==4) {
                            fileStore = new S3Bucket(cmd.getOptionValue("r"), cmd.getOptionValue("b"), cmd.getOptionValue("a"), cmd.getOptionValue("s"));
                        }else{
                            logger.debug("Missing "+required.stream().filter(c->!cmd.hasOption(c)).collect(Collectors.joining(", ")));
                            System.exit(1);
                        }
                        break;
                }
            }
            kafkaClient = new Client(cmd.getOptionValue("k"), cmd.getOptionValue("g"));
            String path = cmd.getOptionValue("p");
            String topicName = cmd.getOptionValue("t");
            switch(cmd.getOptionValue("o")){
                case "c":
                case "changes":
                    try {
                        if(FileStructure.exists(path)){
                            SyncFull latestSync = FileStructure.openLastSync(path);
                            Changes changes = FileStructure.localChanges(path, latestSync.getFolder());
                            FileStructure.displayChanges(changes);
                        } else {
                            logger.debug("Non synced directory, assuming the directory is a new sync.");
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "preview":
                case "p":
                    Consumer.ConsumerResponse consumerResponse = kafkaClient.getConsumer().exportLatestFull(topicName);
                    if(consumerResponse !=null) {
                        logger.info("Consumed");
                        Changes changes = FileStructure.localChanges(path, consumerResponse.getFolder());
                        FileStructure.displayChanges(changes);
                    }else{
                        logger.info("No folder sync data to compare to.");
                    }
                    break;
                case "listen":
                case "l":
                    if(FileStructure.exists(path)) { // sync file exists?
                        SyncFull lastSync = FileStructure.openLastSync(path);
                        StoreDriver finalFileStore3 = fileStore;
                        String shellCommand = cmd.getOptionValue("e");
                        Long offsetFull = kafkaClient.getConsumer().getLatestOffsetFull(topicName);
                        boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
                        kafkaClient.getConsumer().listen(topicName, lastSync.getOffsetChanges(), changes->{
                            try {
                                System.out.println("Applying changes to path: "+path);
                                FileStructure.displayChanges(changes.getChanges());
                                FileStructure.delete(path, changes.getChanges().getDeletions());
                                FileStructure.mkdir(path, changes.getChanges().getAdditions());
                                FileStructure.collapseFolder(path, changes.getChanges().getAdditions()).parallelStream().forEach(file->{
                                    finalFileStore3.download((String)file[0], (String)file[1]);
                                    new File((String)file[0]).setLastModified((long)file[2]);
                                });
                                FileStructure.collapseFolder(path, changes.getChanges().getModifications()).parallelStream().forEach(file->{
                                    new File((String)file[0]).delete();
                                    finalFileStore3.download((String)file[0], (String)file[1]);
                                    new File((String)file[0]).setLastModified((long)file[2]);
                                });
                                FileStructure.applyChange(lastSync.getFolder(), changes.getChanges());
                                FileStructure.save(path, new SyncFull(offsetFull, changes.getOffsetChanges(), lastSync.getFolder()));
                                if(shellCommand!=null){
                                    Process process = null;
                                    if (isWindows) {

                                        process = Runtime.getRuntime().exec(String.format("powershell.exe \'\"%s\" %s\'", shellCommand,path));
                                    }else{
                                        process = Runtime.getRuntime().exec(String.format("%s %s",shellCommand,path));
                                    }
                                    Executors.newSingleThreadExecutor().submit(new StreamLog(process.getErrorStream(), c->{
                                        System.out.println(c);
                                    }));
                                    Executors.newSingleThreadExecutor().submit(new StreamLog(process.getInputStream(), c->{
                                        System.out.println(c);
                                    }));
                                    int exitCode = process.waitFor();
                                    System.out.println("Exit code: "+exitCode);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                    break;
                case "grab":
                case "g":
                    Changes changes;
                    SyncFull localRoot;
                    if(FileStructure.exists(path)) { // sync file exists?
                        localRoot = FileStructure.openLastSync(path);
                        Changes changesFromLastSync = FileStructure.localChanges(path, localRoot.getFolder());
                        Long offsetChanges = kafkaClient.getConsumer().getLatestOffsetChanges(topicName);
                        if(changesFromLastSync.hasChanges()) {
                            System.out.println("Local changes not yet synced to");
                            FileStructure.displayChanges(changesFromLastSync);
                            System.exit(1);
                        }
                        Consumer.ConsumerResponse exportLatest = kafkaClient.getConsumer().exportLatestFull(topicName);
                        Changes changesFromLatest = FileStructure.remoteChanges(localRoot.getFolder(), exportLatest.getFolder());

                        if(!changesFromLatest.hasChanges()) {
                            System.out.println("Path up to date!");
                            System.exit(1);
                        }

                        if(!changesFromLatest.hasChanges()){
                            System.out.println("Nothing to export.");
                        }else {
                            FileStructure.displayChanges(changesFromLatest);
                            InputStreamReader inputStream = new InputStreamReader(System.in);
                            BufferedReader reader =  new BufferedReader(inputStream);
                            System.out.println("Full export offset: "+exportLatest.getOffset());
                            System.out.println("Changes offset: "+offsetChanges);
                            System.out.print("Confirm grab [y/n]: ");
                            switch (reader.readLine()) {
                                case "y":
                                    StoreDriver finalFileStore = fileStore;
                                    FileStructure.delete(path, changesFromLatest.getDeletions());
                                    FileStructure.mkdir(path, changesFromLatest.getAdditions());
                                    FileStructure.collapseFolder(path, changesFromLatest.getAdditions()).parallelStream().forEach(file->{
                                        finalFileStore.download((String)file[0], (String)file[1]);
                                        new File((String)file[0]).setLastModified((long)file[2]);
                                    });
                                    FileStructure.collapseFolder(path, changesFromLatest.getModifications()).parallelStream().forEach(file->{
                                        new File((String)file[0]).delete();
                                        finalFileStore.download((String)file[0], (String)file[1]);
                                        new File((String)file[0]).setLastModified((long)file[2]);
                                    });
                                    FileStructure.save(path, new SyncFull(exportLatest.getOffset(), offsetChanges, exportLatest.getFolder()));
                                    break;
                                default:
                                case "n":
                                    System.out.println("Cancelled export");
                                    break;
                            }
                            reader.close();
                            inputStream.close();
                        }
                    }else{
                        //full grab
                        Consumer.ConsumerResponse localRootFolder = kafkaClient.getConsumer().exportLatestFull(topicName);
                        Long offsetChanges = kafkaClient.getConsumer().getLatestOffsetChanges(topicName);
                        if(offsetChanges==null){
                            System.out.println("Changes topic empty, error.");
                            System.exit(1);
                        }
                        InputStreamReader inputStream = new InputStreamReader(System.in);
                        BufferedReader reader =  new BufferedReader(inputStream);
                        System.out.println("Full export offset: "+localRootFolder.getOffset());
                        System.out.println("Changes offset: "+offsetChanges);
                        System.out.print("Confirm new path export [y/n]: ");
                        switch (reader.readLine()) {
                            case "y":
                                StoreDriver finalFileStore = fileStore;
                                FileStructure.mkdir(path, localRootFolder.getFolder());
                                FileStructure.collapseFolder(path, localRootFolder.getFolder()).parallelStream().forEach(file->{
                                    finalFileStore.download((String)file[0], (String)file[1]);
                                    new File((String)file[0]).setLastModified((long)file[2]);
                                });

                                FileStructure.save(path, new SyncFull(localRootFolder.getOffset(), offsetChanges, localRootFolder.getFolder()));
                                break;
                            default:
                            case "n":
                                System.out.println("Cancelled export");
                                break;
                        }
                        reader.close();
                        inputStream.close();

                    }
                    break;
                case "export":
                case "e":
                    if(FileStructure.exists(path)) {

                        localRoot = FileStructure.openLastSync(path);
                        Changes changesFromLastSync = FileStructure.localChanges(path, localRoot.getFolder());


                        FileStructure.applyChange(localRoot.getFolder(), changesFromLastSync);

                        Consumer.ConsumerResponse exportLatest = kafkaClient.getConsumer().exportLatestFull(topicName);
                        Changes changesFromLatest = FileStructure.remoteChanges(exportLatest.getFolder(), localRoot.getFolder());
                        if(changesFromLastSync.hasChanges()) {
                            System.out.println("Changes from last sync");
                            FileStructure.displayChanges(changesFromLastSync);
                        }
                        if(changesFromLatest.hasChanges()) {
                            System.out.println("Changes from latest export on topic.");
                            FileStructure.displayChanges(changesFromLatest);
                        }

                        changes = FileStructure.remoteChanges(exportLatest.getFolder(), localRoot.getFolder());

                        if(localRoot.equals(exportLatest)) {
                            System.out.println("Sync error, files updated locally that changed remotely.");
                        }else if(!changesFromLatest.hasChanges()){
                            System.out.println("Nothing to export.");
                        }else {
                            InputStreamReader inputStream = new InputStreamReader(System.in);
                            BufferedReader reader =  new BufferedReader(inputStream);
                            System.out.print("Confirm export [y/n]: ");
                            switch (reader.readLine()) {
                                case "y":
                                    StoreDriver finalFileStore = fileStore;
                                    FileStructure.collapseChanges(path, changesFromLatest).parallelStream().forEach(file->finalFileStore.upload((String)file[0], (String)file[1]));
                                    long[] offsets = kafkaClient.getProducer().push(topicName, changes, localRoot.getFolder());
                                    FileStructure.save(path, new SyncFull(offsets[0], offsets[1], localRoot.getFolder()));
                                    break;
                                default:
                                case "n":
                                    System.out.println("Cancelled export");
                                    break;
                            }
                            reader.close();
                            inputStream.close();
                        }
                    }else{
                        BufferedReader reader =  new BufferedReader(new InputStreamReader(System.in));
                        System.out.print("Generate and publish new folder to topic? [y/n]: ");
                        switch(reader.readLine()){
                            case "y":
                                Folder localRootFolder = FileStructure.generate(path);
                                StoreDriver finalFileStore2 = fileStore;
                                FileStructure.collapseFolder(path, localRootFolder).parallelStream().forEach(file-> finalFileStore2.upload((String)file[0], (String)file[1]));
                                long[] offsets = kafkaClient.getProducer().push(topicName, FileStructure.remoteChanges(new Folder(), localRootFolder), localRootFolder);
                                FileStructure.save(path, new SyncFull(offsets[0], offsets[1], localRootFolder));
                                break;
                            default:
                            case "n":
                                System.out.println("Cancelled export");
                                break;
                        }
                    }
                    break;
                case "exportForce":
                case "ef":
                    changes = null;
                    Folder localRootFolder = FileStructure.generate(path);
                    if(FileStructure.exists(path)) {
                        SyncFull lastSync = FileStructure.openLastSync(path);
                        changes = FileStructure.remoteChanges(lastSync.getFolder(), localRootFolder);
                    }
                    BufferedReader reader =  new BufferedReader(new InputStreamReader(System.in));
                    System.out.print("Confirm export [y/n]: ");
                    switch(reader.readLine()){
                        case "y":
                            StoreDriver finalFileStore1 = fileStore;
                            FileStructure.collapseFolder(path, localRootFolder).parallelStream().forEach(file->finalFileStore1.upload((String)file[0], (String)file[1]));
                            long[] offsets = kafkaClient.getProducer().push(topicName, changes, localRootFolder);
                            FileStructure.save(path, new SyncFull(offsets[0], offsets[1], localRootFolder));
                            break;
                        default:
                        case "n":
                            System.out.println("Cancelled export");
                            break;
                    }
                    break;
            }
        } catch (ParseException | IOException e) {
            e.printStackTrace();
            logger.debug(e.getMessage());
            formatter.printHelp("utility-name", generateOptions());
            System.exit(1);
        }
    }
}
