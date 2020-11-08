package com.synload.filestore.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.filestore.structure.Changes;
import com.synload.filestore.structure.Folder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    Properties props;

    public Producer(Properties props) {
        this.props = props;
    }
    public long[] push(String topicName, Changes changes, Folder folder) throws JsonProcessingException {
        Properties properties = new Properties();
        properties.putAll(this.props);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 78643200);

        long time = System.currentTimeMillis();
        long elapsedTime = 0;

        long[] offsets = new long[2];

        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);
        RecordMetadata outMetadata;
        try {
            if(folder!=null) {
                String folderString = new ObjectMapper().writeValueAsString(folder);
                final ProducerRecord<Long, String> fullRecord = new ProducerRecord<>(topicName + "_full", time, folderString);
                outMetadata = producer.send(fullRecord).get();
                elapsedTime = System.currentTimeMillis() - time;
                offsets[0] = outMetadata.offset()+1;
                System.out.printf("sent record(key=%s) " + "meta(partition=%d, offset=%d) time=%d\n", fullRecord.key(), outMetadata.partition(), outMetadata.offset(), elapsedTime);
            }
            if(changes!=null){
                String changesString = new ObjectMapper().writeValueAsString(changes);
                final ProducerRecord<Long, String> changesRecord = new ProducerRecord<>(topicName+"_changes", time, changesString);
                outMetadata = producer.send(changesRecord).get();
                elapsedTime = System.currentTimeMillis() - time;
                offsets[1] = outMetadata.offset()+1;
                System.out.printf("sent record(key=%s) " + "meta(partition=%d, offset=%d) time=%d\n", changesRecord.key(), outMetadata.partition(), outMetadata.offset(), elapsedTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
        return offsets;
    }
}
