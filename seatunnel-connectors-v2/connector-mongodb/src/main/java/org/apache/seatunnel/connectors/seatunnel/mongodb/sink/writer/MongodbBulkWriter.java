package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Writer for MongoDB sink. */
public class MongodbBulkWriter
        implements SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> {

    private final MongoClientProvider collectionProvider;

    private final transient MongoCollection<Document> collection;

    private final ConcurrentLinkedQueue<Document> currentBulk = new ConcurrentLinkedQueue<>();

    private final List<DocumentBulk> pendingBulks = new ArrayList<>();

    private final DocumentSerializer<SeaTunnelRow> serializer;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    private transient volatile Exception flushException;

    private final long maxSize;

    private final boolean flushOnCheckpoint;

    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);

    private transient volatile boolean closed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongodbBulkWriter.class);

    public MongodbBulkWriter(
            DocumentSerializer<SeaTunnelRow> serializer, MongodbWriterOptions options) {
        this.collectionProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(options.getConnectString())
                        .database(options.getDatabase())
                        .collection(options.getCollection())
                        .build();
        this.collection = collectionProvider.getDefaultCollection();
        this.serializer = serializer;
        this.maxSize = options.getFlushSize();
        this.flushOnCheckpoint = options.isFlushOnCheckpoint();
        if (!flushOnCheckpoint && options.getFlushInterval().getSeconds() > 0) {
            this.scheduler = Executors.newScheduledThreadPool(1);
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongodbBulkWriter.this) {
                                    if (true && !closed) {
                                        try {
                                            rollBulkIfNeeded(true);
                                            flush();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            options.getFlushInterval().get(ChronoUnit.SECONDS),
                            options.getFlushInterval().get(ChronoUnit.SECONDS),
                            TimeUnit.SECONDS);
        }
    }

    private synchronized void flush() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            while (iterator.hasNext()) {
                DocumentBulk bulk = iterator.next();
                do {
                    try {
                        // ordered, non-bypass mode
                        if (bulk.size() > 0) {
                            collection.insertMany(bulk.getDocuments());
                        }
                        iterator.remove();
                        break;
                    } catch (MongoException e) {
                        // maybe partial failure
                        LOGGER.error("Failed to flush data to MongoDB", e);
                    }
                } while (!closed && retryPolicy.shouldBackoffRetry());
            }
        }
    }

    private void ensureConnection() {
        try {
            collection.listIndexes();
        } catch (MongoException e) {
            LOGGER.warn("Connection is not available, try to reconnect", e);
            collectionProvider.recreateClient();
        }
    }

    @NotThreadSafe
    class RetryPolicy {

        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                // exit backoff
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to MongoDB", flushException);
        }
    }

    @Override
    public void write(SeaTunnelRow o) throws IOException {
        checkFlushException();
        currentBulk.add(serializer.serialize(o));
        rollBulkIfNeeded();
    }

    @Override
    public Optional<MongodbCommitInfo> prepareCommit() throws IOException {
        if (flushOnCheckpoint) {
            rollBulkIfNeeded(true);
        }

        return Optional.of(new MongodbCommitInfo(pendingBulks));
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }

    @Override
    public List<DocumentBulk> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    private synchronized void rollBulkIfNeeded(boolean force) {
        int size = currentBulk.size();

        if (force || size >= maxSize) {
            DocumentBulk bulk = new DocumentBulk(maxSize);
            for (int i = 0; i < size; i++) {
                if (bulk.size() >= maxSize) {
                    pendingBulks.add(bulk);
                    bulk = new DocumentBulk(maxSize);
                }
                bulk.add(currentBulk.poll());
            }
            pendingBulks.add(bulk);
        }
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        // flush all cache data before close in non-transaction mode
        if (!flushOnCheckpoint) {
            synchronized (this) {
                if (!closed) {
                    try {
                        rollBulkIfNeeded(true);
                        flush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }

        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }

        if (collectionProvider != null) {
            collectionProvider.close();
        }
    }
}
