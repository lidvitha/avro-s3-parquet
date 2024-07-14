package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class S3SinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

    private AmazonS3 s3Client;
    private String bucketName;
    private Map<String, List<GenericRecord>> topicBuffers;
    private Map<String, Long> topicLastFlushTimes;
    private int batchSize;
    private long batchTimeMs;
    private String schemaRegistryUrl;
    private SchemaRegistryClient schemaRegistryClient;
    private final Map<String, Schema> schemaCache = new HashMap<>();

    @Override
    public void start(Map<String, String> props) {
        // Log all properties
        props.forEach((key, value) -> log.info("Property: {} = {}", key, value));

        String accessKeyId = props.get(S3SinkConfig.AWS_ACCESS_KEY_ID);
        String secretAccessKey = props.get(S3SinkConfig.AWS_SECRET_ACCESS_KEY);
        bucketName = props.get(S3SinkConfig.S3_BUCKET_NAME);
        schemaRegistryUrl = props.get(S3SinkConfig.SCHEMA_REGISTRY_URL);

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(props.get(S3SinkConfig.S3_REGION)))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        topicBuffers = new HashMap<>();
        topicLastFlushTimes = new HashMap<>();

        batchSize = Integer.parseInt(props.get(S3SinkConfig.S3_BATCH_SIZE));
        batchTimeMs = Long.parseLong(props.get(S3SinkConfig.S3_BATCH_TIME_MS));

        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            String topic = record.topic();
            try {
                Schema schema = getSchemaForTopic(topic);
                GenericRecord avroRecord = validateAvroPayload(record.value(), schema);
                topicBuffers.computeIfAbsent(topic, k -> new ArrayList<>()).add(avroRecord);
            } catch (Exception e) {
                log.error("Failed to process record: {}", record, e);
            }

            if (shouldFlush(topic)) {
                flushRecords(topic);
            }
        }
    }

    private Schema getSchemaForTopic(String topic) throws Exception {
        if (schemaCache.containsKey(topic)) {
            return schemaCache.get(topic);
        }
        String subject = topic + "-value";
        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        Schema schema = new Schema.Parser().parse(schemaMetadata.getSchema());
        schemaCache.put(topic, schema);
        return schema;
    }

    private GenericRecord validateAvroPayload(Object value, Schema schema) throws Exception {
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            org.apache.avro.generic.GenericRecordBuilder recordBuilder = new org.apache.avro.generic.GenericRecordBuilder(schema);

            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                Object fieldValue = struct.get(field.name());
                recordBuilder.set(field, fieldValue);
            }

            return recordBuilder.build();
        } else if (value instanceof byte[]) {
            byte[] payload = (byte[]) value;
            DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
            return reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
        } else {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getName());
        }
    }

    private boolean shouldFlush(String topic) {
        List<GenericRecord> buffer = topicBuffers.get(topic);
        if (buffer == null) {
            return false;
        }
        return buffer.size() >= batchSize ||
                (System.currentTimeMillis() - topicLastFlushTimes.getOrDefault(topic, 0L)) >= batchTimeMs;
    }

    private void flushRecords(String topic) {
        if (topicBuffers.containsKey(topic) && !topicBuffers.get(topic).isEmpty()) {
            try {
                File tempFile = File.createTempFile("kafka-parquet-", ".parquet");
                tempFile.deleteOnExit();

                Path path = new Path(tempFile.getAbsolutePath());
                ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                        .withSchema(getSchemaForTopic(topic))
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withConf(new Configuration())
                        .build();

                for (GenericRecord record : topicBuffers.get(topic)) {
                    writer.write(record);
                }

                writer.close();

                String key = String.format("%s/%s", topic, generateFileKey());
                s3Client.putObject(new PutObjectRequest(bucketName, key, tempFile));
                topicBuffers.get(topic).clear();

                tempFile.delete();
            } catch (Exception e) {
                log.error("Failed to flush records for topic: {}", topic, e);
            }
        }
    }

    private String generateFileKey() {
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        return String.format("event-%s.parquet", timestamp);
    }

    @Override
    public void stop() {
        for (String topic : topicBuffers.keySet()) {
            flushRecords(topic);
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}