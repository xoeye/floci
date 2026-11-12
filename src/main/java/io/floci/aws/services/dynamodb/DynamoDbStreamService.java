package io.floci.aws.services.dynamodb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floci.aws.config.EmulatorConfig;
import io.floci.aws.core.common.AwsArnUtils;
import io.floci.aws.core.common.AwsException;
import io.floci.aws.core.storage.AccountAwareStorageBackend;
import io.floci.aws.core.storage.StorageBackend;
import io.floci.aws.core.storage.StorageFactory;
import io.floci.aws.services.dynamodb.model.DynamoDbStreamRecord;
import io.floci.aws.services.dynamodb.model.KeySchemaElement;
import io.floci.aws.services.dynamodb.model.StreamDescription;
import io.floci.aws.services.dynamodb.model.TableDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class DynamoDbStreamService {

    private static final Logger LOG = Logger.getLogger(DynamoDbStreamService.class);

    /** The shard ID of the first (and, with the default shard count of 1, only) shard. */
    static final String SHARD_ID = shardIdOf(0);
    static final int MAX_RECORDS = 1000;
    private static final String ZERO_SEQUENCE_NUMBER = "000000000000000000000";

    private static final DateTimeFormatter STREAM_LABEL_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    /** Keyed by table ARN, which is unique per partition, region, account and table. */
    private final ConcurrentHashMap<String, StreamDescription> streams = new ConcurrentHashMap<>();
    /** Per-shard record deques, keyed by {@code streamArn + "|shard|" + shardIndex}. */
    private final ConcurrentHashMap<String, ConcurrentLinkedDeque<DynamoDbStreamRecord>> records =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> streamRecordCounts = new ConcurrentHashMap<>();
    /** Per stream (the stream ARN embeds account + region + table) so accounts never share a counter. */
    private final ConcurrentHashMap<String, AtomicLong> sequenceCounters = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper;
    final int shardCount;

    @Inject
    public DynamoDbStreamService(ObjectMapper objectMapper, StorageFactory storageFactory,
                                  EmulatorConfig config) {
        this(objectMapper, storageFactory.create("dynamodb", "dynamodb-tables.json",
                new TypeReference<Map<String, TableDefinition>>() {}),
                config.services().dynamodb().streamShardCount());
    }

    /** Package-private constructor for testing. */
    DynamoDbStreamService(ObjectMapper objectMapper, StorageBackend<String, TableDefinition> tableStore) {
        this(objectMapper, tableStore, 1);
    }

    /** Package-private constructor for testing with a configurable shard count. */
    DynamoDbStreamService(ObjectMapper objectMapper, StorageBackend<String, TableDefinition> tableStore,
                          int shardCount) {
        this.objectMapper = objectMapper;
        this.shardCount = Math.max(1, shardCount);
        loadPersistedStreams(tableStore);
    }


    private void loadPersistedStreams(StorageBackend<String, TableDefinition> tableStore) {
        if (tableStore == null) {
            return;
        }
        // No request scope at startup, so keys() would only see the default account's tables.
        if (tableStore instanceof AccountAwareStorageBackend<TableDefinition> aware) {
            aware.scanAllAccountsRaw().values().forEach(this::loadPersistedStream);
            return;
        }
        for (String tableKey : tableStore.keys()) {
            tableStore.get(tableKey).ifPresent(this::loadPersistedStream);
        }
    }

    private void loadPersistedStream(TableDefinition table) {
        if (table.isStreamEnabled()) {
            enableStream(table.getTableName(), table.getTableArn(), table.getStreamViewType(),
                    AwsArnUtils.parse(table.getTableArn()).region(), table.getStreamArn());
        }
    }

    public StreamDescription enableStream(String tableName, String tableArn, String viewType, String region) {
        return enableStream(tableName, tableArn, viewType, region, null);
    }

    public StreamDescription enableStream(String tableName, String tableArn, String viewType, String region, String streamArnInput) {
        StreamDescription existing = streams.get(tableArn);
        if (existing != null && "ENABLED".equals(existing.getStreamStatus())) {
            // Re-enabling a live stream with a different view type retargets it. The records this
            // stream emits are built from the description's view type, so leaving it untouched
            // would keep producing the old shape while the table reports the requested one.
            if (viewType != null && !viewType.equals(existing.getStreamViewType())) {
                existing.setStreamViewType(viewType);
                LOG.infov("Stream view type for table {0} in region {1} is now {2}",
                        tableName, region, viewType);
            }
            return existing;
        }

        String streamArn = streamArnInput;
        Instant now = Instant.now();
        String label;
        if (streamArn == null){
            label = STREAM_LABEL_FORMAT.format(now);
            streamArn = tableArn + "/stream/" + label;
        }
        else {
            label = streamArn.split("/stream/", 2)[1];
        }

        StreamDescription sd = new StreamDescription();
        sd.setStreamArn(streamArn);
        sd.setStreamLabel(label);
        sd.setStreamStatus("ENABLED");
        sd.setStreamViewType(viewType);
        sd.setTableName(tableName);
        sd.setCreationDateTime(now);
        sd.setStartingSequenceNumber(String.format("%021d", 1));

        streams.put(tableArn, sd);
        for (int i = 0; i < shardCount; i++) {
            records.put(shardDequeKey(streamArn, i), new ConcurrentLinkedDeque<>());
            streamRecordCounts.put(shardDequeKey(streamArn, i), new AtomicLong());
        }
        LOG.infov("Enabled stream for table {0} in region {1}: {2} ({3} shards)",
                tableName, region, streamArn, shardCount);
        return sd;
    }

    public void disableStream(String tableArn) {
        StreamDescription sd = streams.get(tableArn);
        if (sd != null) {
            sd.setStreamStatus("DISABLED");
            LOG.infov("Disabled stream for table {0}", tableArn);
        }
    }

    public void deleteStream(String tableArn) {
        StreamDescription sd = streams.remove(tableArn);
        if (sd != null) {
            for (int i = 0; i < shardCount; i++) {
                records.remove(shardDequeKey(sd.getStreamArn(), i));
                streamRecordCounts.remove(shardDequeKey(sd.getStreamArn(), i));
            }
            sequenceCounters.remove(sd.getStreamArn());
            LOG.infov("Deleted stream for table {0}", tableArn);
        }
    }

    /** Emulator reset: drops every stream and its records. Sequence numbers stay monotonic. */
    public void clear() {
        streams.clear();
        records.clear();
        streamRecordCounts.clear();
    }

    public void captureEvent(String eventName, JsonNode oldItem, JsonNode newItem,
                             TableDefinition table, String region) {
        captureEvent(eventName, oldItem, newItem, table, region, false);
    }

    /** {@code ttlDeletion} marks a removal made by the time to live sweep rather than by a caller. */
    public void captureEvent(String eventName, JsonNode oldItem, JsonNode newItem,
                             TableDefinition table, String region, boolean ttlDeletion) {
        StreamDescription sd = streams.get(table.getTableArn());
        if (sd == null || !"ENABLED".equals(sd.getStreamStatus())) {
            return;
        }

        JsonNode sourceItem = newItem != null ? newItem : oldItem;
        ObjectNode keys = buildKeys(sourceItem, table);

        // Assign to shard by hashing the partition key, matching real DynamoDB behavior.
        int shardIndex = shardIndexForKeys(keys, table);

        String viewType = sd.getStreamViewType();
        JsonNode newImage = buildImage(newItem, viewType, true);
        JsonNode oldImage = buildImage(oldItem, viewType, false);

        DynamoDbStreamRecord record = new DynamoDbStreamRecord();
        record.setEventId(UUID.randomUUID().toString());
        record.setEventVersion("1.1");
        record.setEventName(eventName);
        record.setEventSource("aws:dynamodb");
        record.setAwsRegion(region);
        record.setApproximateCreationDateTime(Instant.now().getEpochSecond());
        record.setKeys(keys);
        record.setNewImage(newImage);
        record.setOldImage(oldImage);
        record.setStreamViewType(viewType);
        record.setTtlDeletion(ttlDeletion);

        String dequeKey = shardDequeKey(sd.getStreamArn(), shardIndex);
        ConcurrentLinkedDeque<DynamoDbStreamRecord> deque = records.get(dequeKey);
        if (deque != null) {
            synchronized (deque) {
                long seq = sequenceCounters.computeIfAbsent(sd.getStreamArn(), k -> new AtomicLong())
                        .incrementAndGet();
                record.setSequenceNumber(String.format("%021d", seq));
                streamRecordCounts.computeIfAbsent(dequeKey, ignored -> new AtomicLong())
                        .incrementAndGet();
                deque.addLast(record);
                while (deque.size() > MAX_RECORDS) {
                    deque.pollFirst();
                }
            }
        }
    }

    /** Hash the partition key value to a shard index. Items with the same PK always land on the same shard. */
    private int shardIndexForKeys(ObjectNode keys, TableDefinition table) {
        if (shardCount <= 1) {
            return 0;
        }
        String hashKeyName = null;
        for (KeySchemaElement ks : table.getKeySchema()) {
            if ("HASH".equals(ks.getKeyType())) {
                hashKeyName = ks.getAttributeName();
                break;
            }
        }
        if (hashKeyName == null || !keys.has(hashKeyName)) {
            return 0;
        }
        // Use the string representation of the key value for hashing.
        int hash = keys.get(hashKeyName).toString().hashCode();
        return Math.floorMod(hash, shardCount);
    }

    private ObjectNode buildKeys(JsonNode item, TableDefinition table) {
        ObjectNode keys = objectMapper.createObjectNode();
        if (item == null) {
            return keys;
        }
        for (KeySchemaElement ks : table.getKeySchema()) {
            String attrName = ks.getAttributeName();
            if (item.has(attrName)) {
                keys.set(attrName, item.get(attrName));
            }
        }
        return keys;
    }

    private JsonNode buildImage(JsonNode item, String viewType, boolean isNewImage) {
        if (item == null) {
            return null;
        }
        return switch (viewType) {
            case "KEYS_ONLY" -> null;
            case "NEW_IMAGE" -> isNewImage ? item : null;
            case "OLD_IMAGE" -> !isNewImage ? item : null;
            case "NEW_AND_OLD_IMAGES" -> item;
            default -> null;
        };
    }

    public List<StreamDescription> listStreams(String tableNameFilter, String accountId, String region) {
        List<StreamDescription> result = new ArrayList<>();
        for (StreamDescription sd : streams.values()) {
            if (tableNameFilter != null && !tableNameFilter.equals(sd.getTableName())) {
                continue;
            }
            AwsArnUtils.Arn streamArn = AwsArnUtils.parse(sd.getStreamArn());
            if (!accountId.equals(streamArn.accountId()) || !region.equals(streamArn.region())) {
                continue;
            }
            result.add(sd);
        }
        return result;
    }

    StreamDescription describeStream(String streamArn) {
        for (StreamDescription sd : streams.values()) {
            if (streamArn.equals(sd.getStreamArn())) {
                return sd;
            }
        }
        throw new AwsException("ResourceNotFoundException",
                "Stream not found: " + streamArn, 400);
    }

    /** The shard IDs this stream has. */
    List<String> shardIds() {
        List<String> ids = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            ids.add(shardIdOf(i));
        }
        return ids;
    }

    String getShardIterator(String streamArn, String shardId,
                            String iteratorType, String sequenceNumber) {
        StreamDescription sd = describeStream(streamArn);
        if (!"ENABLED".equals(sd.getStreamStatus()) && !"DISABLED".equals(sd.getStreamStatus())) {
            throw new AwsException("ResourceNotFoundException",
                    "Stream not found: " + streamArn, 400);
        }

        int shardIndex = shardIndexOf(shardId);
        String dequeKey = shardDequeKey(streamArn, shardIndex);
        ConcurrentLinkedDeque<DynamoDbStreamRecord> deque = records.get(dequeKey);
        List<DynamoDbStreamRecord> snapshot;
        long recordCount;
        if (deque == null) {
            snapshot = List.of();
            recordCount = 0;
        } else {
            synchronized (deque) {
                snapshot = new ArrayList<>(deque);
                recordCount = streamRecordCounts.getOrDefault(dequeKey, new AtomicLong()).get();
            }
        }

        String cursorSequence = switch (iteratorType) {
            case "TRIM_HORIZON" -> snapshot.isEmpty() ? zeroSequence() : snapshot.get(0).getSequenceNumber();
            case "LATEST" -> snapshot.isEmpty() ? zeroSequence() : snapshot.get(snapshot.size() - 1).getSequenceNumber();
            case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER" -> {
                if (sequenceNumber == null || sequenceNumber.isBlank()) {
                    throw new AwsException("ValidationException",
                            "Sequence number is required for this iterator type", 400);
                }
                yield sequenceNumber;
            }
            default -> throw new AwsException("ValidationException",
                    "Unknown iterator type: " + iteratorType, 400);
        };

        boolean inclusive = "TRIM_HORIZON".equals(iteratorType) || "AT_SEQUENCE_NUMBER".equals(iteratorType);
        long cursorRecordCount = zeroSequence().equals(cursorSequence) ? recordCount : -1;
        return encodeIterator(streamArn, shardIndex, cursorSequence, inclusive, cursorRecordCount);
    }

    private int findSequencePosition(List<DynamoDbStreamRecord> records, String targetSeq, boolean inclusive) {
        for (int i = 0; i < records.size(); i++) {
            String seq = records.get(i).getSequenceNumber();
            int cmp = seq.compareTo(targetSeq);
            if (inclusive ? cmp >= 0 : cmp > 0) {
                return i;
            }
        }
        return records.size();
    }

    record GetRecordsResult(List<DynamoDbStreamRecord> records, String nextShardIterator) {}

    GetRecordsResult getRecords(String shardIterator, Integer limit) {
        String[] parts = decodeIterator(shardIterator);
        String streamArn = parts[0];
        int shardIndex = Integer.parseInt(parts[1]);
        String cursorSequence = parts[2];
        boolean inclusive = Boolean.parseBoolean(parts[3]);
        long cursorRecordCount = parseRecordCount(parts[4]);

        String dequeKey = shardDequeKey(streamArn, shardIndex);
        ConcurrentLinkedDeque<DynamoDbStreamRecord> deque = records.get(dequeKey);
        List<DynamoDbStreamRecord> snapshot;
        long currentRecordCount;
        if (deque == null) {
            snapshot = List.of();
            currentRecordCount = 0;
        } else {
            synchronized (deque) {
                snapshot = new ArrayList<>(deque);
                currentRecordCount = streamRecordCounts.getOrDefault(dequeKey, new AtomicLong()).get();
            }
        }
        int position = findSequencePosition(snapshot, cursorSequence, inclusive);
        if (zeroSequence().equals(cursorSequence) && cursorRecordCount >= 0
                && currentRecordCount - cursorRecordCount > MAX_RECORDS) {
            throw new AwsException("TrimmedDataAccessException",
                    "The requested sequence number has been trimmed", 400);
        }
        if (!snapshot.isEmpty() && !zeroSequence().equals(cursorSequence)
                && cursorSequence.compareTo(snapshot.get(0).getSequenceNumber()) < 0) {
            throw new AwsException("TrimmedDataAccessException",
                    "The requested sequence number has been trimmed", 400);
        }

        int effectiveLimit = limit != null ? limit : 100;
        int end = Math.min(position + effectiveLimit, snapshot.size());
        List<DynamoDbStreamRecord> page = snapshot.subList(position, end);

        String nextSequence = page.isEmpty()
                ? cursorSequence
                : page.get(page.size() - 1).getSequenceNumber();
        long nextRecordCount = zeroSequence().equals(nextSequence) ? cursorRecordCount : -1;
        String nextIterator = encodeIterator(streamArn, shardIndex, nextSequence, page.isEmpty() && inclusive, nextRecordCount);
        return new GetRecordsResult(new ArrayList<>(page), nextIterator);
    }

    /** Encode a shard iterator with the shard index embedded. */
    private String encodeIterator(String streamArn, int shardIndex, String sequenceNumber,
                                   boolean inclusive, long recordCount) {
        String raw = streamArn + "|" + shardIndex + "|" + sequenceNumber + "|" + inclusive + "|" + recordCount;
        return Base64.getEncoder().encodeToString(raw.getBytes());
    }

    /** The stream an iterator reads, rejecting a malformed iterator as GetRecords does. */
    String streamArnOf(String shardIterator) {
        return decodeIterator(shardIterator)[0];
    }

    private String[] decodeIterator(String iterator) {
        try {
            String raw = new String(Base64.getDecoder().decode(iterator));
            // Format: streamArn|shardIndex|sequenceNumber|inclusive|recordCount
            // Parse from the right since streamArn can contain pipes in theory.
            int lastPipe = raw.lastIndexOf('|');
            int inclusivePipe = raw.lastIndexOf('|', lastPipe - 1);
            int sequencePipe = raw.lastIndexOf('|', inclusivePipe - 1);
            int shardPipe = raw.lastIndexOf('|', sequencePipe - 1);
            if (shardPipe < 0 || sequencePipe < 0 || inclusivePipe < 0 || lastPipe < 0
                    || lastPipe == raw.length() - 1) {
                throw new AwsException("ValidationException", "Invalid shard iterator", 400);
            }
            String shardIndex = raw.substring(shardPipe + 1, sequencePipe);
            String inclusive = raw.substring(inclusivePipe + 1, lastPipe);
            String recordCount = raw.substring(lastPipe + 1);
            if (!"true".equals(inclusive) && !"false".equals(inclusive)) {
                throw new AwsException("ValidationException", "Invalid shard iterator", 400);
            }
            try {
                Integer.parseInt(shardIndex);
                Long.parseLong(recordCount);
            } catch (NumberFormatException e) {
                throw new AwsException("ValidationException", "Invalid shard iterator", 400);
            }
            return new String[]{raw.substring(0, shardPipe), shardIndex,
                    raw.substring(sequencePipe + 1, inclusivePipe), inclusive, recordCount};
        } catch (IllegalArgumentException e) {
            throw new AwsException("ValidationException", "Invalid shard iterator", 400);
        }
    }

    private long parseRecordCount(String recordCount) {
        try {
            return Long.parseLong(recordCount);
        } catch (NumberFormatException e) {
            throw new AwsException("ValidationException", "Invalid shard iterator", 400);
        }
    }

    private String zeroSequence() {
        return ZERO_SEQUENCE_NUMBER;
    }

    // ── shard helpers ──────────────────────────────────────────

    /** Build a shard ID from a zero-based index: {@code shardId-0000000001-00000000NNN}. */
    static String shardIdOf(int index) {
        return String.format("shardId-0000000001-%011d", index + 1);
    }

    /** Parse the zero-based shard index from a shard ID. */
    private int shardIndexOf(String shardId) {
        if (shardId == null || !shardId.startsWith("shardId-")) {
            throw new AwsException("ResourceNotFoundException",
                    "Shard " + shardId + " does not exist", 400);
        }
        try {
            int idx = Integer.parseInt(shardId.substring(shardId.lastIndexOf('-') + 1)) - 1;
            if (idx < 0 || idx >= shardCount) {
                throw new AwsException("ResourceNotFoundException",
                        "Shard " + shardId + " does not exist", 400);
            }
            return idx;
        } catch (NumberFormatException e) {
            throw new AwsException("ResourceNotFoundException",
                    "Shard " + shardId + " does not exist", 400);
        }
    }

    /** Key for the per-shard record deque map. */
    private static String shardDequeKey(String streamArn, int shardIndex) {
        return streamArn + "|shard|" + shardIndex;
    }
}
