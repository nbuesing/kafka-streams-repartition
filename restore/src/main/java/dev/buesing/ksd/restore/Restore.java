package dev.buesing.ksd.restore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.buesing.ksd.common.domain.ProductStatsV1;
import dev.buesing.ksd.tools.serde.JsonDeserializer;
import dev.buesing.ksd.tools.serde.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

@Slf4j
public class Restore {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .setTimeZone(TimeZone.getDefault())
                    .registerModule(new JavaTimeModule());

    private RocksDB rocksDB;

    private final Options options;

    public Restore(final Options options) {
        this.options = options;
    }

    public void start() {

        try (KafkaConsumer<String, ProductStatsV1> consumer = new KafkaConsumer<>(consumer(options))) {

            consumer.subscribe(Collections.singleton(options.getChangelogTopic()));

            initRocksDB();

            boolean noLag = false;

            while (!noLag) {

                log.info("Consuming");

                final ConsumerRecords<String, ProductStatsV1> records = consumer.poll(Duration.ofMillis(500L));

                try {
                    List<PartitionInfo> p = consumer.partitionsFor(options.getChangelogTopic());
                    noLag = p.stream().map(i -> new TopicPartition(i.topic(), i.partition())).allMatch(t -> {
                        final long lag = consumer.currentLag(t).orElse(-1);
                        log.debug("partition={}, current-lag={}", t.partition(), lag);
                        return lag == 0;
                    });
                } catch (final Exception e) {
                    log.info("unable to get partition info or consumer lag for topic={}", options.getChangelogTopic());
                }

                records.forEach(record -> {
                    final String key = record.key();
                    log.info("storing key={}", key);
                    try {
                        rocksDB.put(key.getBytes(), toBytes(record.value()));
                    } catch (final RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

        }

        try (KafkaProducer<String, ProductStatsV1> producer = new KafkaProducer<>(producer(options))) {

            log.info("Producing");

            // iterate over the entire RocksDB state-store.
            RocksIterator iterator = rocksDB.newIterator();

            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                final String key = new String(iterator.key(), StandardCharsets.UTF_8);
                final ProductStatsV1 value = fromBytes(iterator.value());

                log.info("Sending key={}", key);
                producer.send(new ProducerRecord<>(options.getVersionedProductPurchasedRestore(), null, key, value, null), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("error producing to kafka", exception);
                    } else {
                        log.debug("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            }

            iterator.close();
        }

        log.info("fully completed.");

    }

    private Map<String, Object> consumer(final Options options) {
        return Map.ofEntries(
                Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers()),
                Map.entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName()),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                Map.entry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId())
        );
    }

    private Map<String, Object> producer(final Options options) {
        return Map.ofEntries(
                Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers()),
                Map.entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName()),
                Map.entry(ProducerConfig.ACKS_CONFIG, "all")
        );
    }


    private void initRocksDB() {
        RocksDB.loadLibrary();
        final org.rocksdb.Options options = new org.rocksdb.Options();
        options.setCreateIfMissing(true);
        final File dbDir = new File(this.options.getStateDir(), this.options.getGroupId());
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            rocksDB = RocksDB.open(options, dbDir.getAbsolutePath());
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] toBytes(final ProductStatsV1 value) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(value);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static ProductStatsV1 fromBytes(final byte[] value) {
        try {
            return OBJECT_MAPPER.readValue(value, ProductStatsV1.class);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
