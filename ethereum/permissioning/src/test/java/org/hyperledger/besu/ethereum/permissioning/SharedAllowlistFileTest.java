/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.permissioning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.verifyConfigFileMatchesState;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.ALLOWLIST_TYPE;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The node and account allowlists in one file, as with the default {@code permissions_config.toml}
 * in the data directory, each changed through its own controller.
 */
class SharedAllowlistFileTest {
  private static final String NODE = "enode://" + "1".repeat(128) + "@192.168.0.1:30303";
  private static final String REVOKED_NODE = "enode://" + "2".repeat(128) + "@192.168.0.2:30303";
  private static final String ACCOUNT = "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73";

  @TempDir private Path directory;
  private Path file;

  @BeforeEach
  void writeFile() throws Exception {
    file = directory.resolve("permissions_config.toml");
    Files.writeString(
        file,
        "nodes-allowlist=[\""
            + NODE
            + "\"]\n"
            // the account as it may be written by hand, in mixed case
            + "accounts-allowlist=[\"0xFe3b557E8Fb62b89F4916B721be55cEb828dBd73\"]\n");
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void changesToOneAllowlistDoNotUndoConcurrentChangesToTheOther() throws Exception {
    final NodeLocalConfigPermissioningController nodes = nodeController();
    final AccountLocalConfigPermissioningController accounts = accountController();
    final int rounds = 100;
    final CyclicBarrier start = new CyclicBarrier(2);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final List<Object> results = new ArrayList<>();
    try {
      final Future<List<AllowlistOperationResult>> nodeResults =
          executor.submit(
              () -> {
                start.await();
                final List<AllowlistOperationResult> list = new ArrayList<>();
                for (int i = 0; i < rounds; i++) {
                  list.add(nodes.addNodes(List.of(REVOKED_NODE)).result());
                  list.add(nodes.removeNodes(List.of(REVOKED_NODE)).result());
                }
                return list;
              });
      final Future<List<AllowlistOperationResult>> accountResults =
          executor.submit(
              () -> {
                start.await();
                final List<AllowlistOperationResult> list = new ArrayList<>();
                for (int i = 0; i < rounds; i++) {
                  final String account = String.format("0x%040x", i + 1);
                  list.add(accounts.addAccounts(List.of(account)));
                  list.add(accounts.removeAccounts(List.of(account)));
                }
                return list;
              });
      results.addAll(nodeResults.get());
      results.addAll(accountResults.get());
    } finally {
      executor.shutdownNow();
    }

    assertThat(results).containsOnly(AllowlistOperationResult.SUCCESS);
    // the revoked node stays revoked on disk, and the file holds both allowlists as they are
    assertThat(verifyConfigFileMatchesState(ALLOWLIST_TYPE.NODES, List.of(NODE), file)).isTrue();
    assertThat(verifyConfigFileMatchesState(ALLOWLIST_TYPE.ACCOUNTS, List.of(ACCOUNT), file))
        .isTrue();
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentChangesToOneAllowlistAllSucceed() throws Exception {
    // each change checks the file against the allowlist it started from, so changes to one
    // allowlist must not overlap
    final NodeLocalConfigPermissioningController nodes = nodeController();
    final int threads = 4;
    final int nodesPerThread = 25;
    final CyclicBarrier start = new CyclicBarrier(threads);
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    final List<Future<List<AllowlistOperationResult>>> futures = new ArrayList<>();
    try {
      for (int t = 0; t < threads; t++) {
        final int thread = t;
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  final List<AllowlistOperationResult> list = new ArrayList<>();
                  for (int i = 0; i < nodesPerThread; i++) {
                    final String node =
                        String.format(
                            "enode://%0128x@10.0.%d.%d:30303", thread * 1000 + i + 10, thread, i);
                    list.add(nodes.addNodes(List.of(node)).result());
                  }
                  return list;
                }));
      }
      final List<AllowlistOperationResult> results = new ArrayList<>();
      for (final Future<List<AllowlistOperationResult>> future : futures) {
        results.addAll(future.get());
      }
      assertThat(results).containsOnly(AllowlistOperationResult.SUCCESS);
    } finally {
      executor.shutdownNow();
    }

    assertThat(nodes.getNodesAllowlist()).hasSize(1 + threads * nodesPerThread);
    assertThat(verifyConfigFileMatchesState(ALLOWLIST_TYPE.NODES, nodes.getNodesAllowlist(), file))
        .isTrue();
  }

  @Test
  void nodeAddedToTheFileByHandIsNotDiscardedByAChangeMadeThroughTheController() throws Exception {
    final NodeLocalConfigPermissioningController nodes = nodeController();
    final String nodeAddedByHand = "enode://" + "3".repeat(128) + "@192.168.0.3:30303";
    Files.writeString(
        file,
        Files.readString(file)
            .replace(
                "nodes-allowlist=[\"" + NODE + "\"]",
                "nodes-allowlist=[\"" + NODE + "\",\"" + nodeAddedByHand + "\"]"));

    assertThat(nodes.addNodes(List.of(REVOKED_NODE)).result())
        .isEqualTo(AllowlistOperationResult.ERROR_ALLOWLIST_FILE_SYNC);
    assertThat(Files.readString(file)).contains(nodeAddedByHand);
  }

  @Test
  void accountAddedToTheFileByHandIsNotDiscardedByAChangeMadeThroughTheController()
      throws Exception {
    final AccountLocalConfigPermissioningController accounts = accountController();
    final String accountAddedByHand = "0x627306090abab3a6e1400e9345bc60c78a8bef57";
    Files.writeString(
        file,
        Files.readString(file)
            .replace(
                "accounts-allowlist=[\"" + ACCOUNT + "\"]",
                "accounts-allowlist=[\"" + ACCOUNT + "\",\"" + accountAddedByHand + "\"]"));

    assertThat(accounts.addAccounts(List.of(String.format("0x%040x", 1))))
        .isEqualTo(AllowlistOperationResult.ERROR_ALLOWLIST_FILE_SYNC);
    assertThat(Files.readString(file)).contains(accountAddedByHand);
  }

  @Test
  void accountsInLowercaseAreLoadedWithoutRewritingTheFile() throws Exception {
    Files.writeString(
        file, "nodes-allowlist=[\"" + NODE + "\"]\naccounts-allowlist=[\"" + ACCOUNT + "\"]\n");
    final String content = Files.readString(file);
    final Object fileKey = Files.readAttributes(file, BasicFileAttributes.class).fileKey();
    Assumptions.assumeTrue(fileKey != null, "files have no identity here");

    final AccountLocalConfigPermissioningController accounts = accountController();
    accounts.reload();

    assertThat(accounts.getAccountAllowlist()).containsExactly(ACCOUNT);
    // replacing the file would have given it a new identity
    assertThat(Files.readAttributes(file, BasicFileAttributes.class).fileKey()).isEqualTo(fileKey);
    assertThat(Files.readString(file)).isEqualTo(content);
  }

  @Test
  void accountsNotInLowercaseAreRewrittenInLowercaseWhenLoaded() throws Exception {
    final AccountLocalConfigPermissioningController accounts = accountController();

    assertThat(accounts.getAccountAllowlist()).containsExactly(ACCOUNT);
    assertThat(verifyConfigFileMatchesState(ALLOWLIST_TYPE.ACCOUNTS, List.of(ACCOUNT), file))
        .isTrue();
  }

  @Test
  void changeIsRejectedWhenTheFileHasTomlErrors() throws Exception {
    final NodeLocalConfigPermissioningController nodes = nodeController();
    final String secondAccount = "0x627306090abab3a6e1400e9345bc60c78a8bef57";
    // a missing comma in the accounts line, edited by hand after the node started
    final String invalid =
        "nodes-allowlist=[\""
            + NODE
            + "\"]\naccounts-allowlist=[\""
            + ACCOUNT
            + "\" \""
            + secondAccount
            + "\"]\n";
    Files.writeString(file, invalid);

    assertThat(nodes.addNodes(List.of(REVOKED_NODE)).result())
        .isEqualTo(AllowlistOperationResult.ERROR_ALLOWLIST_PERSIST_FAIL);
    assertThat(nodes.getNodesAllowlist()).containsExactly(NODE);
    assertThat(Files.readString(file)).isEqualTo(invalid);
  }

  @Test
  void accountsLoadedFromTheFileCanThenBeChanged() throws Exception {
    final AccountLocalConfigPermissioningController accounts = accountController();
    final String newAccount = String.format("0x%040x", 1);

    assertThat(accounts.addAccounts(List.of(newAccount)))
        .isEqualTo(AllowlistOperationResult.SUCCESS);
    assertThat(
            verifyConfigFileMatchesState(
                ALLOWLIST_TYPE.ACCOUNTS, List.of(ACCOUNT, newAccount), file))
        .isTrue();

    accounts.reload();
    assertThat(accounts.removeAccounts(List.of(ACCOUNT)))
        .isEqualTo(AllowlistOperationResult.SUCCESS);
    assertThat(verifyConfigFileMatchesState(ALLOWLIST_TYPE.ACCOUNTS, List.of(newAccount), file))
        .isTrue();
  }

  private LocalPermissioningConfiguration configuration() throws Exception {
    return PermissioningConfigurationBuilder.permissioningConfiguration(
        true, EnodeDnsConfiguration.dnsDisabled(), file.toString(), true, file.toString());
  }

  private NodeLocalConfigPermissioningController nodeController() throws Exception {
    return new NodeLocalConfigPermissioningController(
        configuration(), List.of(), Bytes.wrap(new byte[64]), new NoOpMetricsSystem());
  }

  private AccountLocalConfigPermissioningController accountController() throws Exception {
    return new AccountLocalConfigPermissioningController(configuration(), new NoOpMetricsSystem());
  }
}
