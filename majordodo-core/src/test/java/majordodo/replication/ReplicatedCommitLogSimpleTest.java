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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import majordodo.task.BrokerStatusSnapshot;
import majordodo.task.LogSequenceNumber;
import majordodo.task.StatusEdit;
import majordodo.task.Task;
import org.apache.bookkeeper.client.api.DigestType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Simple tests for FileCommit Log
 *
 * @author enrico.olivelli
 */
public class ReplicatedCommitLogSimpleTest {

    private static final String BROKER_ID = "testBrokerId";

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    private Map<String, byte[]> getCustomMetadata(ReplicatedCommitLog log, long ledgerId) throws Exception {
        return log
            .getBookKeeper().newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withDigestType(DigestType.CRC32C)
                .withPassword(log.getSharedSecret().getBytes(StandardCharsets.UTF_8))
                .execute()
            .get()
            .getLedgerMetadata()
            .getCustomMetadata();
    }
    
    @Test
    public void test() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();
            try (ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), 40000, "/dodo", folderSnapshots.getRoot().toPath(), null, false);) {
                log.getClusterManager().start();
                log.requestLeadership();
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                    fail();
                }, false);
                log.startWriting();
                assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, -1);
                assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, -1);
                assertTrue(snapshot.getTasks().isEmpty());
                StatusEdit edit1 = StatusEdit.ADD_TASK(1, "mytask", "param1", "myuser", 0, 0, 0, null, 0, null, null);
                StatusEdit edit2 = StatusEdit.WORKER_CONNECTED("node1", "psasa", "localhost", new HashSet<>(), System.currentTimeMillis());
                StatusEdit edit3 = StatusEdit.ASSIGN_TASK_TO_WORKER(1, "worker1", 1, "db1,db2");
                StatusEdit edit4 = StatusEdit.TASK_STATUS_CHANGE(1, "node1", Task.STATUS_FINISHED, "theresult");
                LogSequenceNumber logStatusEdit1 = log.logStatusEdit(edit1);
                LogSequenceNumber logStatusEdit2 = log.logStatusEdit(edit2);
                LogSequenceNumber logStatusEdit3 = log.logStatusEdit(edit3);
                LogSequenceNumber logStatusEdit4 = log.logStatusEdit(edit4);
                
                Map<String, byte[]> customMeta = getCustomMetadata(log, logStatusEdit4.ledgerId);

                Assert.assertEquals("majordodo", new String(customMeta.get("application"), StandardCharsets.UTF_8));
                Assert.assertEquals("broker", new String(customMeta.get("component"), StandardCharsets.UTF_8));
                Assert.assertEquals("", new String(customMeta.get("broker-id"), StandardCharsets.UTF_8));
            }
            
            try (ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), 40000, "/dodo", folderSnapshots.getRoot().toPath(), null, false);) {
                log.getClusterManager().start();
                log.requestLeadership();
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                System.out.println("snapshot:" + snapshot);
                // no snapshot was taken...
                assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, -1);
                assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, -1);
                List<StatusEdit> edits = new ArrayList<>();
                AtomicLong last = new AtomicLong(-1);
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                    System.out.println("entry:" + a + ", " + b);

                    assertTrue(a.sequenceNumber > last.get());
                    edits.add(b);
                    last.set(a.sequenceNumber);
                }, false);
                log.startWriting();
                assertEquals(StatusEdit.TYPE_NOOP, edits.get(0).editType);
                assertEquals(StatusEdit.TYPE_ADD_TASK, edits.get(1).editType);
                assertEquals(StatusEdit.TYPE_WORKER_CONNECTED, edits.get(2).editType);
                assertEquals(StatusEdit.TYPE_ASSIGN_TASK_TO_WORKER, edits.get(3).editType);
                assertEquals("db1,db2", edits.get(3).resources);
                assertEquals(StatusEdit.TYPE_TASK_STATUS_CHANGE, edits.get(4).editType);
            }

            try (ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), 40000, "/dodo",
                folderSnapshots.getRoot().toPath(), null, false, Collections.emptyMap(), BROKER_ID);) {

                log.getClusterManager().start();
                log.requestLeadership();
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                }, false);
                log.startWriting();

                StatusEdit edit1 = StatusEdit.ADD_TASK(1, "mytask", "param1", "myuser", 0, 0, 0, null, 0, null, null);
                long ledgerId = log.logStatusEdit(edit1).ledgerId;

                Map<String, byte[]> customMeta = getCustomMetadata(log, ledgerId);

                Assert.assertEquals("majordodo", new String(customMeta.get("application"), StandardCharsets.UTF_8));
                Assert.assertEquals("broker", new String(customMeta.get("component"), StandardCharsets.UTF_8));
                Assert.assertEquals(BROKER_ID, new String(customMeta.get("broker-id"), StandardCharsets.UTF_8));
            }
            
        }
    }

    @Test
    public void testStopBookie() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();
            try (ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), 40000, "/dodo", folderSnapshots.getRoot().toPath(), null, false);) {
                log.getClusterManager().start();
                log.requestLeadership();
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                    fail();
                }, false);
                log.startWriting();
                assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, -1);
                assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, -1);
                assertTrue(snapshot.getTasks().isEmpty());
                StatusEdit edit1 = StatusEdit.ADD_TASK(1, "mytask", "param1", "myuser", 0, 0, 0, null, 0, null, null);
                StatusEdit edit2 = StatusEdit.WORKER_CONNECTED("node1", "psasa", "localhost", new HashSet<>(), System.currentTimeMillis());
                StatusEdit edit3 = StatusEdit.ASSIGN_TASK_TO_WORKER(1, "worker1", 1, "db1,db2");
                StatusEdit edit4 = StatusEdit.TASK_STATUS_CHANGE(1, "node1", Task.STATUS_FINISHED, "theresult");
                LogSequenceNumber logStatusEdit1 = log.logStatusEdit(edit1);
                LogSequenceNumber logStatusEdit2 = log.logStatusEdit(edit2);
                LogSequenceNumber logStatusEdit3 = log.logStatusEdit(edit3);
                LogSequenceNumber logStatusEdit4 = log.logStatusEdit(edit4);
            }

            try (ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), 40000, "/dodo", folderSnapshots.getRoot().toPath(), null, false);) {
                log.getClusterManager().start();
                log.requestLeadership();
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                System.out.println("snapshot:" + snapshot);
                // no snapshot was taken...
                assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, -1);
                assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, -1);
                List<StatusEdit> edits = new ArrayList<>();
                AtomicLong last = new AtomicLong(-1);
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                    System.out.println("entry:" + a + ", " + b);

                    assertTrue(a.sequenceNumber > last.get());
                    edits.add(b);
                    last.set(a.sequenceNumber);
                }, false);
                log.startWriting();
                assertEquals(StatusEdit.TYPE_NOOP, edits.get(0).editType);
                assertEquals(StatusEdit.TYPE_ADD_TASK, edits.get(1).editType);
                assertEquals(StatusEdit.TYPE_WORKER_CONNECTED, edits.get(2).editType);
                assertEquals(StatusEdit.TYPE_ASSIGN_TASK_TO_WORKER, edits.get(3).editType);
                assertEquals("db1,db2", edits.get(3).resources);
                assertEquals(StatusEdit.TYPE_TASK_STATUS_CHANGE, edits.get(4).editType);

            }

        }
    }
}
