package dev.buesing.ksd.restore;

import dev.buesing.ksd.common.domain.ProductStatsV1;
import dev.buesing.ksd.tools.serde.JsonSerde;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

@Slf4j
public class Streams {

    private final static Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> System.err.println("Uncaught exception in thread '" + t.getName() + "': " + e.getMessage());

    private static final Duration SHUTDOWN = Duration.ofSeconds(30);

    private static final Random RANDOM = new Random();

    private Map<String, Object> properties(final Options options) {

        final Map<String, Object> defaults = Map.ofEntries(
                Map.entry(ProducerConfig.LINGER_MS_CONFIG, 100),
                Map.entry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers()),
                Map.entry(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"),
                Map.entry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()),
                Map.entry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName()),
                Map.entry(StreamsConfig.APPLICATION_ID_CONFIG, "restore"),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"),
                Map.entry(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE),
                Map.entry(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class),
                Map.entry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
                //Map.entry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2)
        );

        final Map<String, Object> map = new HashMap<>(defaults);

        try {
            final Properties properties = new Properties();
            final File file = new File("./streams.properties");
            if (file.exists() && file.isFile()) {
                log.info("applying streams.properties");
                properties.load(new FileInputStream(file));
                map.putAll(properties.entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
            }
        } catch (final IOException e) {
            log.info("no streams.properties override file found");
        }

        return map;
    }


    public void start(final Options options) {

        Properties p = toProperties(properties(options));

        final Topology topology = streamsBuilder(options).build(p);

        log.info("Topology:\n" + topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, p);

        streams.setUncaughtExceptionHandler(e -> {
            log.error("unhandled streams exception, shutting down.", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Runtime shutdown hook, state={}", streams.state());
            if (streams.state().isRunningOrRebalancing()) {
                streams.close(SHUTDOWN);
            }
        }));

        streams.start();

        Timer timer = new Timer("foo");

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (streams.state() != KafkaStreams.State.RUNNING) {
                    return;
                }

                try {
                    ReadOnlyKeyValueStore<String, ValueAndTimestamp<ProductStatsV1>> store = streams.store(StoreQueryParameters.fromNameAndType("product-purchased-store", QueryableStoreTypes.timestampedKeyValueStore()));
                    KeyValueIterator<String, ValueAndTimestamp<ProductStatsV1>> iterator = store.all();
                    iterator.forEachRemaining(i -> {
                        System.out.println(i.key + " : " +  i.value.timestamp() + " : " + i.value.value());
                    });
                    iterator.close();
                } catch (final Exception e) {
                    log.error("exception e={}", e.getMessage(), e);
                }

                //store.all().close()...
            }
        }, 5000L, 1000L);


    }

    private StreamsBuilder streamsBuilder(final Options options) {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, ProductStatsV1, KeyValueStore<Bytes, byte[]>> productMaterializedV1 =
                Materialized.as("product-purchased-store");

        builder
                .<String, ProductStatsV1>stream("order-processor-v1-product-purchased-store-changelog", Consumed.as("purchase-order-source"))
                .peek((k, v) -> log.info("processing k={}", k))
                .groupByKey(Grouped.as("groupByKey"))
                .reduce((incoming, aggregate) -> incoming, Named.as("reduce"), productMaterializedV1)
                //.suppress(Suppressed.untilTimeLimit())
                .toStream(Named.as("toStream"))
                .to(options.getVersionedProductPurchasedRestore(), Produced.as("to"));

        return builder;
    }



    private static void dumpRecord(final ConsumerRecord<String, String> record) {
        log.info("Record:\n\ttopic     : {}\n\tpartition : {}\n\toffset    : {}\n\tkey       : {}\n\tvalue     : {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }

    public static Properties toProperties(final Map<String, Object> map) {
        final Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }

    private static void pause(final long duration) {
        try {
            Thread.sleep(duration);
        } catch (final InterruptedException e) {
        }
    }


}

