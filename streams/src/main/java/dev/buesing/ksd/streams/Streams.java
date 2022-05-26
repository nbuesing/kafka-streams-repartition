package dev.buesing.ksd.streams;

import dev.buesing.ksd.common.domain.*;
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
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

@Slf4j
public class Streams {

    private static final Duration SHUTDOWN = Duration.ofSeconds(30);

    private static final Random RANDOM = new Random();

    private Map<String, Object> properties(final Options options) {

        final Map<String, Object> defaults = Map.ofEntries(
                Map.entry(ProducerConfig.LINGER_MS_CONFIG, 100),
                Map.entry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers()),
                Map.entry(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"),
                Map.entry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()),
                Map.entry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName()),
                Map.entry(StreamsConfig.APPLICATION_ID_CONFIG, options.getVersionedApplicationId()),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getAutoOffsetReset()),
                Map.entry(StreamsConfig.CLIENT_ID_CONFIG, options.getClientId()),
                Map.entry(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE),
                Map.entry(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class),
                Map.entry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2),
                Map.entry(StreamsConfig.STATE_DIR_CONFIG, options.getStateDir())
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

        log.info("starting streams : " + options.getClientId());

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

    }

    private StreamsBuilder streamsBuilder(final Options options) {
        final StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, Store> stores = builder.globalTable(options.getStoreTopic(),
                Consumed.as("gktable-stores"),
                Materialized.as("store-global-table")
        );

        KTable<String, User> users = builder.table(options.getUserTopic() + "-" + options.getVersion(),
                Consumed.as("ktable-users"),
                Materialized.as("user-table")
        );

        KTable<String, Product> products = builder.table(options.getProductTopic() + "-" + options.getVersion(),
                Consumed.as("ktable-products"),
                Materialized.as("product-table")
        );

        final Materialized<String, PurchaseOrder, KeyValueStore<Bytes, byte[]>> materialized =
                Materialized.<String, PurchaseOrder, KeyValueStore<Bytes, byte[]>>as("pickup-order-reduce-store");

        // version 1 state-store
        final Materialized<String, ProductStatsV1, KeyValueStore<Bytes, byte[]>> productMaterializedV1 =
                Materialized.<String, ProductStatsV1, KeyValueStore<Bytes, byte[]>>as("product-purchased-store");
        //.withKeySerde(null)
        // .withValueSerde(Serdes.Integer());

        // version 2 state-store
        final Materialized<String, ProductStatsV2, KeyValueStore<Bytes, byte[]>> productMaterializedV2 =
                Materialized.<String, ProductStatsV2, KeyValueStore<Bytes, byte[]>>as("product-purchased-store");
        //.withKeySerde(null)
        //.withValueSerde(Serdes.Long());


        KStream<String, PurchaseOrder> productPurchased = builder
                .<String, PurchaseOrder>stream(options.getPurchaseTopic() + "-" + options.getVersion(), Consumed.as("purchase-order-source"))
                .peek((k, v) -> log.info("processing k={}", k))
                .selectKey((k, v) -> v.getUserId(), Named.as("purchase-order-keyByUserId"))
                .join(users, (purchaseOrder, user) -> {
                    purchaseOrder.setUser(user);
                    return purchaseOrder;
                }, Joined.as("purchase-order-join-user"))
                .join(stores, (k, v) -> v.getStoreId(), (purchaseOrder, store) -> {
                    purchaseOrder.setStore(store);
                    return purchaseOrder;
                }, Named.as("purchase-order-join-store"))
                .flatMap((k, v) -> v.getItems().stream().map(item -> KeyValue.pair(item.getSku(), v)).collect(Collectors.toList()),
                        Named.as("purchase-order-products-flatmap"))
                .join(products, (purchaseOrder, product) -> {
                    purchaseOrder.getItems().stream().filter(item -> item.getSku().equals(product.getSku())).forEach(item -> item.setPrice(product.getPrice()));
                    return purchaseOrder;
                }, Joined.as("purchase-order-join-product"));

        productPurchased
                .groupBy((k, v) -> v.getOrderId(), Grouped.as("pickup-order-groupBy-orderId"))
                .reduce((incoming, aggregate) -> {
                    if (aggregate == null) {
                        aggregate = incoming;
                    } else {
                        final PurchaseOrder purchaseOrder = aggregate;
                        incoming.getItems().stream().forEach(item -> {
                            if (item.getPrice() != null) {
                                purchaseOrder.getItems().stream().filter(i -> i.getSku().equals(item.getSku())).forEach(i -> i.setPrice(item.getPrice()));
                            }
                        });
                    }
                    return aggregate;
                }, Named.as("pickup-order-reduce"), materialized)
                .filter((k, v) -> v.getItems().stream().allMatch(i -> i.getPrice() != null), Named.as("pickup-order-filtered"))
                .toStream(Named.as("pickup-order-reduce-tostream"))
                .to(options.getPickupTopic() + "-" + options.getVersion(), Produced.as("pickup-orders"));


        if ("v1".equals(options.getVersion())) {

            productPurchased
                    .groupByKey(Grouped.as(("product-purchased-groupByKey")))
                    .aggregate(() -> null,
                            (k, purchaseOrder, aggregate) -> {
                                if (aggregate == null) {
                                    aggregate = new ProductStatsV1(k);
                                }
                                aggregate.add(purchaseOrder);
                                return aggregate;
                            },
                            Named.as("product-purchased-aggregate"),
                            productMaterializedV1)
                    .toStream(Named.as("product-purchased-aggregate-toStreams"))
                    .transformValues((ValueTransformerWithKeySupplier<String, ProductStatsV1, ProductStatsV1>) () -> new ValueTransformerWithKey<>() {
                        ProcessorContext context;
                        @Override
                        public void init(ProcessorContext context) {
                            this.context = context;
                        }
                        @Override
                        public ProductStatsV1 transform(String readOnlyKey, ProductStatsV1 value) {
                            System.out.println("thread=" + Thread.currentThread().getName() + " partition=" + context.partition() + " instance=" + System.identityHashCode(this) +  " className=" + this.getClass().getName());
                            return value;
                        }
                        @Override
                        public void close() {
                        }

                    }, Named.as("XXXr"))
                    .to(options.getQuantityPurchasedTopic() + "-" + options.getVersion(), Produced.as("product-purchased-to"));

        } else if ("v2".equals(options.getVersion())) {

            productPurchased
                    .groupByKey(Grouped.as(("product-purchased-groupByKey")))
                    .aggregate(() -> null,
                            (k, purchaseOrder, aggregate) -> {
                                if (aggregate == null) {
                                    aggregate = new ProductStatsV2(k);
                                }
                                aggregate.add(purchaseOrder);
                                return aggregate;
                            },
                            Named.as("product-purchased-aggregate"),
                            productMaterializedV2)
                    .toStream(Named.as("product-purchased-aggregate-toStreams"))
                    .to(options.getQuantityPurchasedTopic() + "-" + options.getVersion(), Produced.as("product-purchased-to"));

            builder.<String, ProductStatsV1>stream(options.getProductPurchasedRestore() + "-" + options.getVersion(), Consumed.as("restore-stream"))
                    .peek((k, v) -> log.info("restoring k={}, v={}, type={}", k, v, v.getClass()), Named.as("restore-peek-start"))
                    .transformValues((ValueTransformerWithKeySupplier<String, ProductStatsV1, ProductStatsV2>) () -> new ValueTransformerWithKey<>() {

                        private KeyValueStore<String, ValueAndTimestamp<ProductStatsV2>> store;

                        @Override
                        public void init(ProcessorContext context) {
                            store = context.getStateStore("product-purchased-store");
                        }

                        @Override
                        public ProductStatsV2 transform(String s, ProductStatsV1 productStatsV1) {

                            System.out.println(Thread.currentThread().getName());

                            ValueAndTimestamp<ProductStatsV2> current = store.get(s);

                            ProductStatsV2 productStatsV2;
                            if (current == null) {
                                productStatsV2 = new ProductStatsV2(s);
                            } else {
                                productStatsV2 = current.value();
                            }

                            productStatsV2.merge(productStatsV1);

                            store.put(s, ValueAndTimestamp.make(productStatsV2, System.currentTimeMillis()));

                            return productStatsV2;
                        }

                        @Override
                        public void close() {
                        }

                    }, Named.as("restore-transformer"), "product-purchased-store")
                    .peek((k, v) -> log.info("restored k={}, v={}, type={}", k, v, v.getClass()), Named.as("restore-peek-end"));

        } else {
            throw new InvalidConfigurationException("unsupported version string");
        }

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

