/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package majordodo.replication;

import static majordodo.network.ConnectionRequestInfo.CLIENT_TYPE_BROKER;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import majordodo.network.BrokerHostData;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.ConnectionRequestInfo;
import majordodo.network.Message;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.task.BrokerStatusSnapshot;
import majordodo.task.LogNotAvailableException;
import majordodo.task.LogSequenceNumber;
import majordodo.task.StatusChangesLog;
import majordodo.task.StatusEdit;
import majordodo.utils.FileUtils;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * Commit log replicated on Apache Bookkeeper
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "https://github.com/spotbugs/spotbugs/issues/756")
public class ReplicatedCommitLog extends StatusChangesLog {

    private static final Logger LOGGER = Logger.getLogger(ReplicatedCommitLog.class.getName());
    private static final long DOWNLOAD_FROM_MASTER_TIMEOUT = Long.parseLong(System.getProperty("majordodo.downloadfrommaster.timeout", "240000"));

    private String sharedSecret = "dodo";
    private BookKeeper bookKeeper;
    private ZKClusterManager zKClusterManager;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ReentrantLock snapshotLock = new ReentrantLock();
    private volatile CommitFileWriter writer;
    private long currentLedgerId = 0;
    private long lastSequenceNumber = -1;
    private Path snapshotsDirectory;
    private LedgersInfo actualLedgersList;
    // these are expected to be configurable at runtime from the EmbeddedBroker
    private volatile int ensembleSize = 1;
    private volatile int writeQuorumSize = 1;
    private volatile int ackQuorumSize = 1;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;
    private long maxLogicalLogFileSize = 1024 * 1024 * 256;
    private long writtenBytes = 0;
    private boolean sslUnsecure = true;
    private final String brokerId;
    
    // Workaround: org.apache.bookkeeper.client.api.BKException.HANDLER is currently not public
    static final Function<Throwable, BKException> BK_EXCEPTION_HANDLER = cause -> {
        if (cause == null) {
            return null;
        }
        if (cause instanceof BKException) {
            return (BKException) cause;
        } else {
            BKException ex = new BKException(BKException.Code.UnexpectedConditionException);
            ex.initCause(cause);
            return ex;
        }
    };
    
    @Override
    public boolean isSslUnsecure() {
        return sslUnsecure;
    }

    @Override
    public void setSslUnsecure(boolean sslUnsecure) {
        this.sslUnsecure = sslUnsecure;
    }

    @Override
    public String getSharedSecret() {
        return sharedSecret;
    }

    @Override
    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret;
    }

    public long getMaxLogicalLogFileSize() {
        return maxLogicalLogFileSize;
    }

    public void setMaxLogicalLogFileSize(long maxLogicalLogFileSize) {
        this.maxLogicalLogFileSize = maxLogicalLogFileSize;
    }

    public LedgersInfo getActualLedgersList() {
        return actualLedgersList;
    }

    private byte[] downloadSnapshotFromMaster(BrokerHostData broker) throws Exception {

        InetSocketAddress addre = broker.getSocketAddress();
        boolean ssl = broker.isSsl();
        String host = addre.getHostName();
        if (host == null) {
            host = addre.getAddress().getHostAddress();
        }
        LOGGER.log(Level.INFO, "Downloading snapshot from " + addre + " ssl=" + ssl
            + ", using hostname " + host + ", sslUnsecure:" + sslUnsecure);
        try (NettyBrokerLocator connector = new NettyBrokerLocator(host, addre.getPort(), broker.isSsl())) {
            connector.setSslUnsecure(sslUnsecure);
            try (Channel channel = connector.connect(new ChannelEventListener() {
                @Override
                public void messageReceived(Message message) {

                }

                @Override
                public void channelClosed() {

                }
            }, brokerConnectionRequestInfo);) {

                Message acceptMessage = Message.SNAPSHOT_DOWNLOAD_REQUEST();
                try {
                    Message connectionResponse = channel.sendMessageWithReply(acceptMessage, DOWNLOAD_FROM_MASTER_TIMEOUT);
                    if (connectionResponse.type == Message.TYPE_SNAPSHOT_DOWNLOAD_RESPONSE) {
                        byte[] data = (byte[]) connectionResponse.parameters.get("data");
                        return data;
                    } else {
                        throw new BrokerRejectedConnectionException("Broker rejected snapshot request, response message:" + connectionResponse);
                    }
                } catch (TimeoutException err) {
                    throw new BrokerNotAvailableException(err);
                }
            }
        }
    }

    protected long getCurrentLedgerId() {
        return currentLedgerId;
    }

    private class CommitFileWriter implements AutoCloseable {

        private WriteHandle out;

        private CommitFileWriter() throws LogNotAvailableException {
            try {
                this.out = FutureUtils.result(
                    bookKeeper.newCreateLedgerOp()
                            .withEnsembleSize(ensembleSize)
                            .withWriteQuorumSize(writeQuorumSize)
                            .withAckQuorumSize(ackQuorumSize)
                            .withDigestType(DigestType.CRC32C)
                            .withPassword(sharedSecret.getBytes(StandardCharsets.UTF_8))
                            .withCustomMetadata(LedgerMetadataUtils.buildBrokerLedgerMetadata(brokerId))
                            .execute(),
                    BK_EXCEPTION_HANDLER);
                writtenBytes = 0;
            } catch (BKException err) {
                throw new LogNotAvailableException(err);
            }
        }
        
        protected void tryDeleteLedgerSuppressingErrors() {
            try {
                FutureUtils.result(
                    bookKeeper.newDeleteLedgerOp()
                            .withLedgerId(getLedgerId())
                            .execute(),
                    BK_EXCEPTION_HANDLER);
            } catch (BKException ex) {
                LOGGER.log(Level.SEVERE, "Cannot delete ledger from metadata " + getLedgerId(), ex);
            }
        }

        public long getLedgerId() {
            return this.out.getId();
        }

         public long writeEntry(StatusEdit edit) throws LogNotAvailableException, BKException {
            long _start = System.currentTimeMillis();
            try {
                byte[] serialize = edit.serialize();
                writtenBytes += serialize.length;
                long res = this.out.append(serialize);
                if (writtenBytes > maxLogicalLogFileSize) {
                    LOGGER.log(Level.SEVERE, "{0} bytes written to ledger. need to open a new one", writtenBytes);
                    openNewLedger();
                }
                return res;
            } catch (BKException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                switch (err.getCode()) {
                    case BKException.Code.LedgerClosedException:
                    case BKException.Code.LedgerFencedException:
                    case BKException.Code.NotEnoughBookiesException:
                        throw err;
                    default:
                        throw new LogNotAvailableException(err);
                }
            } catch (InterruptedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                Thread.currentThread().interrupt();
                throw new LogNotAvailableException(err);
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntry {0} time " + (_end - _start) + " ms", new Object[]{edit});
            }
        }

        @Override
        public void close() throws LogNotAvailableException {
            if (out == null) {
                return;
            }
            try {
                out.close();
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            } finally {
                out = null;
            }
        }

        private List<Long> writeEntries(List<StatusEdit> edits) throws LogNotAvailableException, BKException {
            int size = edits.size();
            if (size == 0) {
                return Collections.emptyList();
            } else if (size == 1) {
                return Arrays.asList(writeEntry(edits.get(0)));
            }
            long _start = System.currentTimeMillis();
            try {
                AtomicReference<Throwable> exception = new AtomicReference<>();
                CountDownLatch latch = new CountDownLatch(edits.size());
                List<Long> res = new ArrayList<>(edits.size());
                for (int i = 0; i < size; i++) {
                    res.add(null);
                }
                for (int i = 0; i < size; i++) {
                    final int index = i;
                    StatusEdit edit = edits.get(i);
                    byte[] serialize = edit.serialize();
                    writtenBytes += serialize.length;
                    
                    this.out.appendAsync(serialize)
                        .whenComplete((entryId, error) -> {
                            if (error != null) {
                                exception.set(error);
                                res.set(index, null);
                                for (StatusEdit edit1 : edits) {
                                    // early exit
                                    latch.countDown();
                                }
                            } else {
                                res.set(index, entryId);
                                latch.countDown();
                            }
                        });
                }
                latch.await();
                if (exception.get() != null) {
                    throw exception.get();
                }
                for (Long l : res) {
                    if (l == null) {
                        throw new RuntimeException("bug ! " + res);
                    }
                }
                if (writtenBytes > maxLogicalLogFileSize) {
                    LOGGER.log(Level.INFO, "{0} bytes written to ledger. need to open a new one", writtenBytes);
                    openNewLedger();
                }
                return res;
            } catch (BKException err) {
                // corner case, if some entry has been written ?? it will be duplicated on retry
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                switch (err.getCode()) {
                    case BKException.Code.LedgerClosedException:
                    case BKException.Code.LedgerFencedException:
                    case BKException.Code.NotEnoughBookiesException:
                        throw err;
                    default:
                        throw new LogNotAvailableException(err);
                }
            } catch (Throwable err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntries {0} time {1} ms", new Object[]{edits.size(), _end - _start});
            }
        }
    }

    private final ConnectionRequestInfo brokerConnectionRequestInfo = new ConnectionRequestInfo() {
        @Override
        public Set<Long> getRunningTaskIds() {
            return Collections.emptySet();
        }

        @Override
        public String getWorkerId() {
            return "broker";
        }

        @Override
        public String getProcessId() {
            return "";
        }

        @Override
        public String getLocation() {
            return "";
        }

        @Override
        public String getSharedSecret() {
            return sharedSecret;
        }

        @Override
        public int getMaxThreads() {
            return 0;
        }

        @Override
        public Map<String, Integer> getMaxThreadsByTaskType() {
            return Collections.emptyMap();
        }

        @Override
        public List<Integer> getGroups() {
            return Collections.emptyList();
        }

        @Override
        public Set<Integer> getExcludedGroups() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, Integer> getResourceLimits() {
            return Collections.emptyMap();
        }

        @Override
        public int getMaxThreadPerUserPerTaskTypePercent() {
            return 0;
        }

        @Override
        public String getClientType() {
            return CLIENT_TYPE_BROKER;
        }

    };

    private final LeaderShipChangeListener leaderShiplistener = new LeaderShipChangeListener() {

        @Override
        public void leadershipLost(String reason) {
            LOGGER.log(Level.SEVERE, "leadershipLost: {0}", reason);
            signalBrokerFailed(new Exception("leadership lost: " + reason));

        }

        @Override
        public void leadershipAcquired() {
            LOGGER.log(Level.INFO, "leadershipAcquired");
        }

    };

    public ReplicatedCommitLog(String zkAddress, int zkTimeout, String zkPath, Path snapshotsDirectory, byte[] localhostdata,
        boolean writeacls) throws Exception {
        this(zkAddress, zkTimeout, zkPath, snapshotsDirectory, localhostdata, writeacls, Collections.emptyMap(), "");
    }
    
    public ReplicatedCommitLog(String zkAddress, int zkTimeout, String zkPath, Path snapshotsDirectory, byte[] localhostdata,
        boolean writeacls, Map<String, String> bookkeeperConfiguration, String brokerId) throws Exception {
        if (localhostdata == null) {
            localhostdata = new byte[0];
        }
        String zkConnString = "zk+null://" + zkAddress + zkPath;
        LOGGER.log(Level.INFO, "Zk conn string {0}, zkTimeout {1}", new Object[]{zkConnString, zkTimeout});
        ClientConfiguration config = new ClientConfiguration()
            .setEnableParallelRecoveryRead(true)
            .setThrottleValue(0)
            .setEnableDigestTypeAutodetection(true)
            .setMetadataServiceUri(zkConnString)
            .setZkTimeout(zkTimeout);
        
        bookkeeperConfiguration.forEach((k, v) -> {
            LOGGER.log(Level.INFO, "extra bookkeeper client property {0}={1}", new Object[]{k, v});
            config.setProperty(k, v);
        });
        
        try {
            this.zKClusterManager = new ZKClusterManager(zkAddress, zkTimeout, zkPath, leaderShiplistener, localhostdata, writeacls);
            this.zKClusterManager.waitForConnection();
            this.bookKeeper = BookKeeper.newBuilder(config).build();
            this.snapshotsDirectory = snapshotsDirectory;
            this.zKClusterManager.start();
        } catch (Exception t) {
            close();
            throw t;
        }
        this.brokerId = brokerId;
    }

    public int getEnsemble() {
        return ensembleSize;
    }

    public void setEnsemble(int ensemble) {
        this.ensembleSize = ensemble;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public void setWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    public void setAckQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
    }

    public long getLedgersRetentionPeriod() {
        return ledgersRetentionPeriod;
    }

    public void setLedgersRetentionPeriod(long ledgersRetentionPeriod) {
        this.ledgersRetentionPeriod = ledgersRetentionPeriod;
    }
    
    private void handleLogStatusEditBKException(BKException err) throws LogNotAvailableException {
        switch (err.getCode()) {
            case BKException.Code.LedgerClosedException:
                LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger", err);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new LogNotAvailableException(ex);
                }
                openNewLedger();
                break;

            case BKException.Code.LedgerFencedException:
                LOGGER.log(Level.SEVERE, "this broker was fenced!", err);
                zKClusterManager.close();
                close();
                signalBrokerFailed(err);
                throw new LogNotAvailableException(err);
            case BKException.Code.NotEnoughBookiesException:
                LOGGER.log(Level.SEVERE, "bookkeeper failure", err);
                zKClusterManager.close();
                close();
                signalBrokerFailed(err);
                throw new LogNotAvailableException(err);
            default:
                throw new LogNotAvailableException(err);
        }
    }

    @Override
    public List<LogSequenceNumber> logStatusEditBatch(List<StatusEdit> edits) throws LogNotAvailableException {
        if (edits.isEmpty()) {
            return Collections.emptyList();
        }
        while (true) {
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
            writeLock.lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                List<Long> newSequenceNumbers = writer.writeEntries(edits);
                lastSequenceNumber = newSequenceNumbers.stream().max(Comparator.naturalOrder()).get();
                List<LogSequenceNumber> res = new ArrayList<>();
                for (Long newSequenceNumber : newSequenceNumbers) {
                    res.add(new LogSequenceNumber(currentLedgerId, newSequenceNumber));
                }
                return res;
            } catch (BKException err) {
                handleLogStatusEditBKException(err);
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public LogSequenceNumber logStatusEdit(StatusEdit edit) throws LogNotAvailableException {
        while (true) {
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
            writeLock.lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                long newSequenceNumber = writer.writeEntry(edit);
                lastSequenceNumber = newSequenceNumber;
                return new LogSequenceNumber(currentLedgerId, newSequenceNumber);
            } catch (BKException err) {
                handleLogStatusEditBKException(err);
            } finally {
                writeLock.unlock();
            }
        }

    }

    private void openNewLedger() throws LogNotAvailableException {
        writeLock.lock();
        try {
            closeCurrentWriter();
            writer = new CommitFileWriter();
            currentLedgerId = writer.getLedgerId();
            LOGGER.log(Level.INFO, "Opened new ledger:" + currentLedgerId);
            // #160: workaround to prevent BookKeeper fault if a Bookie goes down when there are no entries on the ledger
            boolean done = false;
            try {
                writer.writeEntry(StatusEdit.NOOP());
                done = true;
            } catch (BKException t) {
                throw new LogNotAvailableException(t);
            } finally {
                if (!done) {
                    LOGGER.log(Level.SEVERE, "Something went wrong while writing on ledeger " + currentLedgerId + ". Trying to delete it");
                    writer.tryDeleteLedgerSuppressingErrors();
                }
            }
            actualLedgersList.addLedger(currentLedgerId);
            zKClusterManager.saveActualLedgersList(actualLedgersList);
        } catch (LogNotAvailableException t) {
            LOGGER.log(Level.SEVERE, "error", t);
            throw t;
        } finally {
            writeLock.unlock();
        }
    }
    
    public ZKClusterManager getClusterManager() {
        return zKClusterManager;
    }

    public BookKeeper getBookKeeper() {
        return bookKeeper;
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer, boolean fencing) throws LogNotAvailableException {
        this.actualLedgersList = zKClusterManager.getActualLedgersList();
        LOGGER.log(Level.INFO, "Actual ledgers list:" + actualLedgersList);
        this.currentLedgerId = snapshotSequenceNumber.ledgerId;
        LOGGER.log(Level.INFO, "Latest snapshotSequenceNumber:" + snapshotSequenceNumber);
        if (currentLedgerId > 0 && !this.actualLedgersList.getActiveLedgers().contains(currentLedgerId)) {
            // TODO: download snapshot from another remote broker
            throw new LogNotAvailableException(new Exception("Actual ledgers list does not include latest snapshot ledgerid:" + currentLedgerId + ". manual recoveryis needed (pickup a recent snapshot from a live broker please)"));
        }
        try {
            for (long ledgerId : actualLedgersList.getActiveLedgers()) {

                if (ledgerId < snapshotSequenceNumber.ledgerId) {
                    LOGGER.log(Level.INFO, "Skipping ledger " + ledgerId);
                    continue;
                }
                
                try (ReadHandle handle = FutureUtils.result(
                        bookKeeper.newOpenLedgerOp()
                                .withDigestType(DigestType.CRC32C)
                                .withLedgerId(ledgerId)
                                .withPassword(sharedSecret.getBytes(StandardCharsets.UTF_8))
                                .withRecovery(fencing)
                                .execute(),
                        BK_EXCEPTION_HANDLER)) {
                    
                    long first;
                    if (ledgerId == snapshotSequenceNumber.ledgerId) {
                        first = snapshotSequenceNumber.sequenceNumber;
                        LOGGER.log(Level.INFO, "Recovering from latest snapshot ledger {0}, starting from entry {1}", new Object[]{ledgerId, first});
                    } else {
                        first = 0;
                        LOGGER.log(Level.INFO, "Recovering from ledger {0}, starting from entry {1}", new Object[]{ledgerId, first});
                    }
                    long lastAddConfirmed = handle.getLastAddConfirmed();
                    LOGGER.log(Level.INFO, "Recovering from ledger " + ledgerId + ", first=" + first, " lastAddConfirmed=" + lastAddConfirmed);
                    final int BATCH_SIZE = 10000;
                    if (lastAddConfirmed >= 0) {

                        for (long b = first; b <= lastAddConfirmed;) {
                            long start = b;
                            long end = b + BATCH_SIZE;
                            if (end > lastAddConfirmed) {
                                end = lastAddConfirmed;
                            }
                            b = end + 1;
                            double percent = ((start - first) * 100.0 / (lastAddConfirmed + 1));
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.log(Level.FINE, "From entry {0}, to entry {1} ({2} %)", new Object[]{start, end, percent});                            
                            }
                            
                            Iterator<LedgerEntry> entries = handle.read(start, end).iterator();
                            while (entries.hasNext()) {
                                LedgerEntry entry = entries.next();
                                LogSequenceNumber number = new LogSequenceNumber(ledgerId, entry.getEntryId());
                                StatusEdit statusEdit = StatusEdit.read(entry.getEntryBytes());
                                if (number.after(snapshotSequenceNumber)) {
                                    LOGGER.log(Level.FINEST, "RECOVER ENTRY {0}, {1}", new Object[]{number, statusEdit});
                                    consumer.accept(number, statusEdit);
                                } else {
                                    LOGGER.log(Level.FINEST, "SKIP ENTRY {0}<{1}, {2}", new Object[]{number, snapshotSequenceNumber, statusEdit});
                                }
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException | BKException err) {
            LOGGER.log(Level.SEVERE, "Fatal error during recovery", err);
            signalBrokerFailed(err);
            throw new LogNotAvailableException(err);
        } catch (Exception err) {
            LOGGER.log(Level.SEVERE, "Unknown fatal error during recovery", err);
            signalBrokerFailed(err);
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        actualLedgersList = zKClusterManager.getActualLedgersList();
        zKClusterManager.ensureLeaderRole();
        openNewLedger();
    }

    @Override
    public void clear() throws LogNotAvailableException {
        this.currentLedgerId = 0;
        try {
            FileUtils.cleanDirectory(snapshotsDirectory);
            zKClusterManager.saveActualLedgersList(new LedgersInfo());
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        ensureDirectories();
    }

    @Override
    public boolean isWritable() {
        return writer != null;
    }

    private void ensureDirectories() throws LogNotAvailableException {
        try {
            if (!Files.isDirectory(snapshotsDirectory)) {
                Files.createDirectories(snapshotsDirectory);
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void checkpoint(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {
        snapshotLock.lock();
        try {
            Path snapshotfilename = writeSnapshotOnDisk(snapshotData);

            deleteOldSnapshots(snapshotfilename);
        } finally {
            snapshotLock.unlock();
        }

        if (zKClusterManager != null && zKClusterManager.isLeader()) {
            dropOldLedgers(snapshotData.getActualLogSequenceNumber());
        }
    }

    private void deleteOldSnapshots(Path snapshotfilename) throws LogNotAvailableException {
        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(snapshotsDirectory)) {
            for (Path path : allfiles) {
                String other_filename = path.getFileName() + "";
                if (other_filename.endsWith(SNAPSHOTFILEXTENSION)) {
                    LOGGER.log(Level.INFO, "Processing snapshot file: " + path);
                    try {
                        other_filename = other_filename.substring(0, other_filename.length() - SNAPSHOTFILEXTENSION.length());

                        int pos = other_filename.indexOf('_');
                        if (pos > 0) {
                            if (!snapshotfilename.equals(path)) {
                                LOGGER.log(Level.INFO, "Deleting old snapshot file: " + path);
                                Files.delete(path);
                            }
                        }
                    } catch (NumberFormatException invalidName) {
                        LOGGER.log(Level.SEVERE, "Error:" + invalidName, invalidName);
                    }
                }
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    private Path writeSnapshotOnDisk(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {
        ensureDirectories();
        LogSequenceNumber actualLogSequenceNumber = snapshotData.getActualLogSequenceNumber();
        String filename = actualLogSequenceNumber.ledgerId + "_" + actualLogSequenceNumber.sequenceNumber;
        Path snapshotfilename_tmp = snapshotsDirectory.resolve(filename + SNAPSHOTFILEXTENSION + ".tmp");
        Path snapshotfilename = snapshotsDirectory.resolve(filename + SNAPSHOTFILEXTENSION);
        LOGGER.log(Level.INFO, "checkpoint, file:{0}", snapshotfilename.toAbsolutePath());

        try (OutputStream out = Files.newOutputStream(snapshotfilename_tmp);
            BufferedOutputStream bout = new BufferedOutputStream(out, 64 * 1024);
            GZIPOutputStream zout = new GZIPOutputStream(bout)) {
            BrokerStatusSnapshot.serializeSnapshot(snapshotData, zout);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        try {
            Files.move(snapshotfilename_tmp, snapshotfilename, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        return snapshotfilename;
    }

    private void dropOldLedgers(LogSequenceNumber latestSnapshotPosition) throws LogNotAvailableException {
        if (ledgersRetentionPeriod > 0) {
            long min_timestamp = System.currentTimeMillis() - ledgersRetentionPeriod;
            List<Long> oldLedgers;
            writeLock.lock();
            try {
                oldLedgers = actualLedgersList.getOldLedgers(min_timestamp);
            } finally {
                writeLock.unlock();
            }
            if (oldLedgers.isEmpty()) {
                return;
            }
            LOGGER.log(Level.INFO, "dropping ledgers before {0}, oldLedgers {1}, currentLedgerId:{2}, latestSnapshotLedgerId:{3}", new Object[]{new java.sql.Timestamp(min_timestamp), oldLedgers, currentLedgerId, latestSnapshotPosition.ledgerId});
            for (long ledgerId : oldLedgers) {
                if (ledgerId >= latestSnapshotPosition.ledgerId
                    || ledgerId >= currentLedgerId) {
                    LOGGER.log(Level.SEVERE, "ledger {0} cannot be dropped", ledgerId);
                    continue;
                }

                writeLock.lock();
                boolean done = false;
                try {
                    LOGGER.log(Level.INFO, "remove ledger {0} from the actualLedgersList", ledgerId);
                    actualLedgersList.removeLedger(ledgerId);
                    zKClusterManager.saveActualLedgersList(actualLedgersList);
                    LOGGER.log(Level.INFO, "dropping ledger {0} on BookKeeper", ledgerId);
                    
                    FutureUtils.result(
                            bookKeeper.newDeleteLedgerOp()
                                .withLedgerId(ledgerId)
                                .execute(),
                            BK_EXCEPTION_HANDLER);
                    done = true;
                } catch (BKException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId + ": " + error, error);
                    if (error.getCode() == BKException.Code.NoSuchLedgerExistsException) {
                        done = true;
                    } else {
                        throw new LogNotAvailableException(error);
                    }
                } catch (LogNotAvailableException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId, error);
                    throw error;
                } finally {
                    if (done) {
                        LOGGER.log(Level.INFO, "dropping ledger {0}, finished", ledgerId);
                    }
                    writeLock.unlock();
                }
            }

        }
    }

    private static final String SNAPSHOTFILEXTENSION = ".snap.json.gz";

    @Override
    public BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException {
        Path snapshotfilename = null;
        ensureDirectories();

        // download a snapshot from the actual leaer if present (this will be generally faster then recoverying from BK)                
        byte[] actualLeader;
        BrokerHostData leaderData = null;
        try {
            actualLeader = zKClusterManager.getActualMaster();
            if (actualLeader != null && actualLeader.length > 0) {
                leaderData = BrokerHostData.parseHostdata(actualLeader);
                LOGGER.log(Level.INFO, "actual leader is at " + leaderData.getHost() + ":" + leaderData.getPort());
            } else {
                LOGGER.log(Level.SEVERE, "no leader is present");
            }

        } catch (Exception err) {
            throw new LogNotAvailableException(err);
        }
        if (leaderData != null && !isLeader()) {
            byte[] snapshot;
            try {
                snapshot = downloadSnapshotFromMaster(leaderData);
                LOGGER.log(Level.INFO, "downloaded " + snapshot.length + " snapshot data from actual leader");
                try (InputStream in = new ByteArrayInputStream(snapshot);
                    GZIPInputStream gzip = new GZIPInputStream(in)) {
                    BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(gzip);
                    writeSnapshotOnDisk(result);
                    currentLedgerId = result.getActualLogSequenceNumber().ledgerId;
                    return result;
                }
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while reading snapshot from network", err);
            }

        }

        LogSequenceNumber latest = null;
        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(snapshotsDirectory)) {
            for (Path path : allfiles) {
                String filename = path.getFileName() + "";
                if (filename.endsWith(SNAPSHOTFILEXTENSION)) {
                    LOGGER.log(Level.INFO, "Processing snapshot file: " + path);
                    try {
                        filename = filename.substring(0, filename.length() - SNAPSHOTFILEXTENSION.length());

                        int pos = filename.indexOf('_');
                        if (pos > 0) {
                            long ledgerId = Long.parseLong(filename.substring(0, pos));
                            long sequenceNumber = Long.parseLong(filename.substring(pos + 1));
                            LOGGER.log(Level.INFO, "File " + path + " contains snapshot, ledgerId:" + ledgerId + ",sequenceNumber:" + sequenceNumber);
                            LogSequenceNumber number = new LogSequenceNumber(ledgerId, sequenceNumber);
                            if (latest == null || number.after(latest)) {
                                latest = number;
                                snapshotfilename = path;
                            }
                        }
                    } catch (NumberFormatException invalidName) {
                        LOGGER.log(Level.SEVERE, "Error:" + invalidName, invalidName);
                    }
                }
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        LedgersInfo _actualLedgersList = zKClusterManager.getActualLedgersList();
        LOGGER.log(Level.INFO, "ActualLedgersList from ZK: " + _actualLedgersList);

        if (snapshotfilename != null) {
            LOGGER.log(Level.INFO, "Loading snapshot from " + snapshotfilename);
            try (InputStream in = Files.newInputStream(snapshotfilename);
                BufferedInputStream bin = new BufferedInputStream(in);
                GZIPInputStream gzip = new GZIPInputStream(bin)) {
                BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(gzip);
                currentLedgerId = result.getActualLogSequenceNumber().ledgerId;

                LOGGER.log(Level.INFO,
                    "Snapshot has been taken at ledgerId=" + result.getActualLogSequenceNumber().ledgerId + ", sequenceNumber=" + result.getActualLogSequenceNumber().sequenceNumber);
                if (_actualLedgersList.getActiveLedgers()
                    .contains(currentLedgerId)) {
                    return result;
                }

                LOGGER.log(Level.SEVERE,
                    "Actually the loaded snapshot is not recoveable given the actual ledgers list. This file cannot be used for recovery");
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "error while reading snapshot data", err);
                throw new LogNotAvailableException(err);
            }
        }

        currentLedgerId = -1;

        if (_actualLedgersList.getFirstLedger() < 0) {
            LOGGER.log(Level.SEVERE, "No snapshot present and no ledger registered on ZK. Starting with a brand new status");
            return new BrokerStatusSnapshot(0, 0, new LogSequenceNumber(-1, -1));
        } else if (_actualLedgersList.getActiveLedgers().contains(_actualLedgersList.getFirstLedger())) {
            LOGGER.log(Level.SEVERE, "No valid snapshot present, But the first ledger of history " + _actualLedgersList.getFirstLedger() + ", is still present in active ledgers list. I can use an empty snapshot in order to boot");
            return new BrokerStatusSnapshot(0, 0, new LogSequenceNumber(-1, -1));
        } else {
            LOGGER.log(Level.SEVERE, "No snapshot present, no leader is present, cannot boot");
            throw new LogNotAvailableException(new Exception("No valid snapshot present, no leader is present, cannot boot"));
        }
    }

    private volatile boolean closed = false;

    @Override
    public final void close() {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closeCurrentWriter();
            if (zKClusterManager != null) {
                try {
                    zKClusterManager.close();
                } finally {
                    zKClusterManager = null;
                }
            }
            closed = true;
            LOGGER.severe("closed");
        } finally {
            writer = null;
            writeLock.unlock();
        }

    }

    private void closeCurrentWriter() {
        if (writer != null) {

            try {
                writer.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while closing ledger", err);
            } finally {
                writer = null;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void requestLeadership() throws LogNotAvailableException {
        zKClusterManager.requestLeadership();
    }

    @Override
    public void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, StatusEdit> consumer) throws LogNotAvailableException {

        List<Long> actualList;
        try {
            actualList = zKClusterManager.getActualLedgersList().getActiveLedgers();
        } catch (LogNotAvailableException temporaryError) {
            LOGGER.log(Level.SEVERE, "temporary error " + temporaryError, temporaryError);
            return;
        }

        List<Long> toRead = actualList;
        if (skipPast.ledgerId != -1) {
            toRead = toRead.stream().filter(l -> l >= skipPast.ledgerId).collect(Collectors.toList());
        }
        
        long nextEntry = skipPast.sequenceNumber + 1;
        LOGGER.log(Level.FINE, "followTheLeader skipPast:{0} toRead: {1} actualList:{2}, nextEntry:{3}", new Object[]{skipPast, toRead, actualList, nextEntry});
        for (Long previous : toRead) {
            //LOGGER.log(Level.SEVERE, "followTheLeader openLedger " + previous + " nextEntry:" + nextEntry);

            List<Map.Entry<Long, StatusEdit>> buffer = new ArrayList<>();

            // first of all we read data from the leader
            try (ReadHandle handler = FutureUtils.result(
                    bookKeeper.newOpenLedgerOp()
                        .withLedgerId(previous)
                        .withDigestType(DigestType.CRC32C)
                        .withPassword(sharedSecret.getBytes(StandardCharsets.UTF_8))
                        .withRecovery(false)
                        .execute(),
                    BK_EXCEPTION_HANDLER)) {
                long lastAddConfirmed = handler.getLastAddConfirmed();
                LOGGER.log(Level.FINE, "followTheLeader openLedger {0} -> lastAddConfirmed:{1}, nextEntry:{2}", new Object[]{previous, lastAddConfirmed, nextEntry});
                if (nextEntry > lastAddConfirmed) {
                    nextEntry = 0;
                    continue;
                }
                try (LedgerEntries entries = handler.read(nextEntry, lastAddConfirmed)) {
                    Iterator<LedgerEntry> entriesIterator = entries.iterator();
                    while (entriesIterator.hasNext()) {
                        LedgerEntry e = entriesIterator.next();
                        long entryId = e.getEntryId();
                        byte[] entryData = e.getEntryBytes();
                        StatusEdit statusEdit = StatusEdit.read(entryData);
                        buffer.add(new AbstractMap.SimpleImmutableEntry<>(entryId, statusEdit));
                    }
                }

                // use the entry
                for (Map.Entry<Long, StatusEdit> entry : buffer) {
                    long entryId = entry.getKey();
                    StatusEdit statusEdit = entry.getValue();
                    LOGGER.log(Level.FINEST, "entry {0},{1} -> {2}", new Object[]{previous, entryId, statusEdit});
                    LogSequenceNumber number = new LogSequenceNumber(previous, entryId);
                    consumer.accept(number, statusEdit);
                    lastSequenceNumber = number.sequenceNumber;
                    currentLedgerId = number.ledgerId;
                }
            } catch (BKException err) {
                switch (err.getCode()) {
                    case BKException.Code.LedgerRecoveryException:
                    case BKException.Code.BookieHandleNotAvailableException:
                        LOGGER.log(Level.SEVERE, "temporary error " + err, err);
                        return;
                    default:
                        throw new LogNotAvailableException(err);
                }
            } catch (InterruptedException err) {
                LOGGER.log(Level.SEVERE, "error while reading ledger " + err, err);
                Thread.currentThread().interrupt();
                throw new LogNotAvailableException(err);
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "error while reading ledger " + err, err);
                throw new LogNotAvailableException(err);
            }
        }
    }

    @Override
    public boolean isLeader() {
        return zKClusterManager != null && zKClusterManager.isLeader();
    }

    @Override
    public LogSequenceNumber getLastSequenceNumber() {
        return new LogSequenceNumber(currentLedgerId, lastSequenceNumber);
    }

}
