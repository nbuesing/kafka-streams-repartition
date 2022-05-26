package dev.buesing.ksd.builder;

import dev.buesing.ksd.common.domain.Product;
import dev.buesing.ksd.common.domain.Store;
import dev.buesing.ksd.common.domain.User;
import dev.buesing.ksd.common.domain.Zip;
import dev.buesing.ksd.tools.serde.JsonSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class BuildSystem {

    private static final short REPLICATION_FACTOR = 3;

    private static final int GLOBAL_PARTITIONS = 1;
    private static final int V1_PARTITIONS = 4;
    private static final int V2_PARTITIONS = 8;


    private static final Map<String, String> CONFIGS = Map.ofEntries(
            Map.entry("retention.ms", "86400000") // 1 day
    );

    private static final Map<String, String> COMPACT_CONFIGS = Map.ofEntries(
            Map.entry("cleanup.policy", "compact"),
            Map.entry("retention.ms", "-1")
    );

    private static final Random RANDOM = new Random();

    private final Options options;
    private final List<Zip> zips = loadZipcodes();

    public BuildSystem(final Options options) {
        this.options = options;
    }

    public void start() {
        createTopics();
        populateTables();
    }


    private NewTopic create(final String topicName, final int partitions, final Map<String, String> configs) {
        final NewTopic topic = new NewTopic(topicName, partitions, REPLICATION_FACTOR);
        topic.configs(configs);
        return topic;
    }

    private void createTopics() {

        final AdminClient admin = KafkaAdminClient.create(properties(options));


        final List<NewTopic> topics = Arrays.asList(
                create(options.getStoreTopic(), GLOBAL_PARTITIONS, COMPACT_CONFIGS),

                create(options.getUserTopic() + "-v1", V1_PARTITIONS, COMPACT_CONFIGS),
                create(options.getProductTopic() + "-v1", V1_PARTITIONS, COMPACT_CONFIGS),
                create(options.getPurchaseTopic() + "-v1", V1_PARTITIONS, CONFIGS),
                create(options.getPickupTopic() + "-v1", V1_PARTITIONS, CONFIGS),
                create(options.getQuantityPurchasedTopic() + "-v1", V1_PARTITIONS, CONFIGS),

                create(options.getUserTopic() + "-v2", V2_PARTITIONS, COMPACT_CONFIGS),
                create(options.getProductTopic() + "-v2", V2_PARTITIONS, COMPACT_CONFIGS),
                create(options.getPurchaseTopic() + "-v2", V2_PARTITIONS, CONFIGS),
                create(options.getPickupTopic() + "-v2", V2_PARTITIONS, CONFIGS),
                create(options.getProductPurchasedRestore() + "-v2", V2_PARTITIONS, CONFIGS),
                create(options.getQuantityPurchasedTopic() + "-v2", V1_PARTITIONS, CONFIGS)
        );


        if (options.isDeleteTopics()) {
            admin.deleteTopics(topics.stream().map(NewTopic::name).collect(Collectors.toList())).topicNameValues().forEach((k, v) -> {
                try {
                    v.get();
                } catch (final InterruptedException | ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        log.debug("not deleting a topic that doesn't exist, topic={}", k);
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        admin.createTopics(topics).values().forEach((k, v) -> {
            try {
                v.get();
            } catch (final InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.warn("{}", e.getCause().getMessage());
                } else {
                    throw new RuntimeException(e);
                }
            }
        });

//        admin.describeTopics(topics.stream().map(NewTopic::name).collect(Collectors.toList())).values().forEach((k, v) -> {
//            try {
//                TopicDescription description = v.get();
//                //TODO -- validate
//            } catch (final InterruptedException | ExecutionException e) {
//                throw new RuntimeException(e);
//            }
//        });

    }

    private void populateTables() {
        final KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(properties(options));

        IntStream.range(0, options.getNumberOfStores()).forEach(i -> {
            Store store = getRandomStore(i);

            log.info("Sending key={}, value={}", store.getStoreId(), store);
            kafkaProducer.send(new ProducerRecord<>(options.getStoreTopic(), null, store.getStoreId(), store), (metadata, exception) -> {
                if (exception != null) {
                    log.error("error producing to kafka", exception);
                } else {
                    log.debug("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        });

        IntStream.range(0, options.getNumberOfUsers()).forEach(i -> {
            User user = getRandomUser(i);

            log.info("Sending key={}, value={}", user.getUserId(), user);
            kafkaProducer.send(new ProducerRecord<>(options.getUserTopic() + "-v1", null, user.getUserId(), user), (metadata, exception) -> {
                if (exception != null) {
                    log.error("error producing to kafka", exception);
                } else {
                    log.debug("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            kafkaProducer.send(new ProducerRecord<>(options.getUserTopic() + "-v2", null, user.getUserId(), user), (metadata, exception) -> {
                if (exception != null) {
                    log.error("error producing to kafka", exception);
                } else {
                    log.debug("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        });


        IntStream.range(0, options.getNumberOfProducts()).forEach(i -> {
            Product product = getRandomProduct(i);

            log.info("Sending key={}, value={}", product.getSku(), product);
            kafkaProducer.send(new ProducerRecord<>(options.getProductTopic() + "-v1", null, product.getSku(), product), (metadata, exception) -> {
                if (exception != null) {
                    log.error("error producing to kafka", exception);
                } else {
                    log.debug("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            kafkaProducer.send(new ProducerRecord<>(options.getProductTopic() + "-v2", null, product.getSku(), product), (metadata, exception) -> {
                if (exception != null) {
                    log.error("error producing to kafka", exception);
                } else {
                    log.debug("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        });

        kafkaProducer.close();

    }

    private Map<String, Object> properties(final Options options) {
        return Map.ofEntries(
                Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers()),
                Map.entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName())
        );
    }

    private User getRandomUser(int userId) {
        final User user = new User();

        user.setUserId(Integer.toString(userId));
        user.setName(generateName());
        user.setEmail(user.getName() + "@foo.com");

        return user;
    }

    private Product getRandomProduct(int sku) {
        final Product product = new Product();

        product.setSku(StringUtils.leftPad(Integer.toString(sku), 10, '0'));
        product.setPrice(BigDecimal.valueOf(RANDOM.nextDouble() * 100.).setScale(2, RoundingMode.HALF_EVEN));

        return product;
    }

    private Store getRandomStore(int storeId) {
        final Store store = new Store();

        store.setStoreId(Integer.toString(storeId));
        store.setName(generateName());

        Zip zip = getRandomZip();

        store.setPostalCode(zip.getZip());
        store.setCity(zip.getCity());
        store.setState(zip.getState());

        return store;
    }

    private Zip getRandomZip() {
        return zips.get(RANDOM.nextInt(zips.size()));
    }

    private static List<Zip> loadZipcodes() {

        //final Map<String, Zip> map = new HashMap<>();
        final List<Zip> list = new ArrayList<>();

        try {

            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');

            InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("zipcodes.csv");
            InputStreamReader reader = new InputStreamReader(input);

            CSVParser parser = new CSVParser(reader, format);

            for (CSVRecord record : parser) {
                final Zip zip = new Zip();
                zip.setZip(record.get("zipcode"));
                zip.setCity(record.get("city"));
                zip.setState(record.get("state_abbr"));
                list.add(zip);
            }

            parser.close();

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        return list;
    }

    private static String generateName() {
        return StringUtils.capitalize(ADJECTIVE[RANDOM.nextInt(ADJECTIVE.length)]) + " " + StringUtils.capitalize(NAMES[RANDOM.nextInt(NAMES.length)]);
    }

    private static final String[] ADJECTIVE = {
            "affable",
            "agreeable",
            "ambitious",
            "amusing",
            "brave",
            "bright",
            "calm",
            "careful",
            "charming",
            "courteous",
            "creative",
            "decisive",
            "determined",
            "diligent",
            "discreet",
            "dynamic",
            "emotional",
            "energetic",
            "exuberant",
            "faithful",
            "fearless",
            "forceful",
            "friendly",
            "funny",
            "generous",
            "gentle",
            "good",
            "helpful",
            "honest",
            "humorous",
            "impartial",
            "intuitive",
            "inventive",
            "loyal",
            "modest",
            "neat",
            "nice",
            "optimistic",
            "patient",
            "persistent",
            "placid",
            "plucky",
            "polite",
            "powerful",
            "practical",
            "quiet",
            "rational",
            "reliable",
            "reserved",
            "sensible",
            "sensitive",
            "shy",
            "sincere",
            "sociable",
            "tidy",
            "tough",
            "versatile",
            "willing",
            "witty"
    };


    private final static String[] NAMES = {

            "Broker",
            "Cleaner",
            "Cluster",
            "CommitLog",
            "Connect",
            "Consumer",
            "Election",
            "Jitter",
            "KIP",
            "Kafka",
            "Leader",
            "Listener",
            "Message",
            "Offset",
            "Partition",
            "Processor",
            "Producer",
            "Protocol",
            "Purgatory",
            "Quota",
            "Replica",
            "Retention",
            "RocksDB",
            "Segment",
            "Schema",
            "Streams",
            "Subject",
            "Timeout",
            "Tombstone",
            "Topic",
            "Topology",
            "Transaction",
            "Zombie",
            "Zookeeper"
    };
}
