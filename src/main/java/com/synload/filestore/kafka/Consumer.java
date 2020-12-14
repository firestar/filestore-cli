package com.synload.filestore.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.filestore.structure.Changes;
import com.synload.filestore.structure.Folder;
import com.synload.filestore.structure.SyncChanges;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
    Properties props;

    public class ConsumerResponse{
        long offset = 0;
        Folder folder;

        public ConsumerResponse(long offset, Folder folder) {
            this.offset = offset;
            this.folder = folder;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public Folder getFolder() {
            return folder;
        }

        public void setFolder(Folder folder) {
            this.folder = folder;
        }
    }
    public Consumer(Properties props) {
        this.props = props;
    }

    public Long getLatestOffset(String topicName){
        Properties properties = new Properties();
        properties.putAll(this.props);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 78643200);
        KafkaConsumer<Long, String> consumer = null;
        Long offset = null;
        try {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topicName));
            consumer.poll(Duration.ofMillis(1000));
            for (Map.Entry<TopicPartition, Long> entry : consumer.endOffsets(consumer.assignment()).entrySet()) {
                offset = entry.getValue();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.close();
        }
        if(offset==null){
            System.out.println("Error offset of changes topic not found.");
            System.exit(1);
        }
        return offset;
    }

    public void listen(String topicName, long offset, ChangeDetection changeDetection){
        Properties properties = new Properties();
        properties.putAll(this.props);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 78643200);
        KafkaConsumer<Long, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topicName+"_changes"));
            consumer.poll(Duration.ofMillis(500));
            for(Map.Entry<TopicPartition, Long> entry: consumer.endOffsets(consumer.assignment()).entrySet()){
                offset = entry.getValue();
                consumer.seek(entry.getKey(), offset);
            }
            while(true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(c->{
                    try {
                        Changes changes = new ObjectMapper().readValue(records.iterator().next().value(), Changes.class);
                        changeDetection.change(new SyncChanges(0, c.offset()+1, changes));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.close();
        }
    }
    public Long getLatestOffsetChanges(String topicName){
        return getLatestOffset(topicName + "_changes");
    }
    public Long getLatestOffsetFull(String topicName){
        return getLatestOffset(topicName + "_full");
    }
    public ConsumerResponse exportLatestChanges(String topicName){
        return exportLatest(topicName + "_changes");
    }
    public ConsumerResponse exportLatestFull(String topicName){
        return exportLatest(topicName + "_full");
    }
    public ConsumerResponse exportLatest(String topicName){
        Folder folder = null;
        Properties properties = new Properties();
        properties.putAll(this.props);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 78643200);
        KafkaConsumer<Long, String> consumer = null;
        long offset = 0;
        try {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topicName));
            consumer.poll(Duration.ofMillis(500));
            for(Map.Entry<TopicPartition, Long> entry: consumer.endOffsets(consumer.assignment()).entrySet()){
                offset = entry.getValue();
                consumer.seek(entry.getKey(), offset-1);
            }
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(500));
            if(records.count()==1){
                folder = new ObjectMapper().readValue(records.iterator().next().value(), Folder.class);
            } else {
                folder = null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.close();
        }
        return new ConsumerResponse(offset, folder);
    }
}
