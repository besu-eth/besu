/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.tests.acceptance.clique;

import static org.apache.logging.log4j.util.LoaderUtil.getClassLoader;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.EthBlock;

public class CliqueToPoSTest extends AcceptanceTestBase {

  private static void copyKeyFile(final BesuNode node) throws IOException {
    final String resourceFileName = "clique/key";
    try (InputStream keyFileStream = getClassLoader().getResourceAsStream(resourceFileName)) {
      if (keyFileStream == null) {
        throw new IOException("Resource not found: " + resourceFileName);
      }
      Path targetPath = node.homeDirectory().resolve("key");
      Files.createDirectories(targetPath.getParent());
      Files.copy(keyFileStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  public static void runBesuCommand(final Path dataPath) throws IOException, InterruptedException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            "../../build/install/besu/bin/besu",
            "--genesis-file",
            "src/acceptanceTest/resources/clique/clique_to_pos.json",
            "--data-path",
            dataPath.toString(),
            "--data-storage-format",
            "BONSAI",
            "blocks",
            "import",
            "src/acceptanceTest/resources/clique/clique.blocks");

    processBuilder.directory(new File(System.getProperty("user.dir")));
    processBuilder.inheritIO(); // This will redirect the output to the console

    Process process = processBuilder.start();
    int exitCode = process.waitFor();
    if (exitCode == 0) {
      System.out.println("Import command executed successfully.");
    } else {
      throw new RuntimeException("Import command execution failed with exit code: " + exitCode);
    }
  }

  private static final MediaType MEDIA_TYPE_JSON =
      MediaType.parse("application/json; charset=utf-8");

  @Test
  public void blocksAreBuiltAndNodesSyncAfterSwitchingToPoS() throws Exception {

    final SnapSyncConfiguration snapServerEnabledConfig =
        ImmutableSnapSyncConfiguration.builder().isSnapServerEnabled(true).build();

    final BesuNode minerNode =
        besu.createNode(
            "miner",
            besuNodeConfigurationBuilder ->
                besuNodeConfigurationBuilder
                    .devMode(false)
                    .genesisConfigProvider(
                        unused ->
                            GenesisConfigurationFactory.createFromResource(
                                "/clique/clique_to_pos.json"))
                    .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                    .engineRpcEnabled(true)
                    .miningEnabled());

    minerNode.setSynchronizerConfiguration(
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.FULL)
            .syncMinimumPeerCount(1)
            .snapSyncConfiguration(snapServerEnabledConfig)
            .build());

    // First sync node uses full sync and starts fresh; it does not produce blocks
    final BesuNode syncNodeFull =
        besu.createNode(
            "full-syncer",
            besuNodeConfigurationBuilder ->
                besuNodeConfigurationBuilder
                    .devMode(false)
                    .genesisConfigProvider(
                        unused ->
                            GenesisConfigurationFactory.createFromResource(
                                "/clique/clique_to_pos.json"))
                    .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                    .synchronizerConfiguration(
                        SynchronizerConfiguration.builder()
                            .syncMode(SyncMode.FULL)
                            .syncMinimumPeerCount(1)
                            .snapSyncConfiguration(snapServerEnabledConfig)
                            .build())
                    .engineRpcEnabled(true));

    // Second sync node uses snap sync and starts fresh; it does not produce blocks
    final BesuNode syncNodeSnap =
        besu.createNode(
            "snap-syncer",
            besuNodeConfigurationBuilder ->
                besuNodeConfigurationBuilder
                    .devMode(false)
                    .genesisConfigProvider(
                        unused ->
                            GenesisConfigurationFactory.createFromResource(
                                "/clique/clique_to_pos.json"))
                    .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                    .engineRpcEnabled(true));

    syncNodeSnap.setSynchronizerConfiguration(
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.SNAP)
            .syncMinimumPeerCount(1)
            .syncPivotDistance(2)
            .snapSyncConfiguration(snapServerEnabledConfig)
            .build());

    // Copy key files to the miner node datadir
    copyKeyFile(minerNode);

    // Import pre-built Clique blocks into the miner's datadir (up to TTD)
    runBesuCommand(minerNode.homeDirectory());

    // Start only the miner; syncNodeFull will be added after the PoS block is produced
    cluster.start(minerNode);

    // Verify the imported blocks are present on the miner
    minerNode.verify(blockchain.currentHeight(4));

    // Build PoS blocks 5-10 on minerNode via Engine API
    ObjectNode latestPayload = null;
    for (int i = 5; i <= 10; i++) {
      latestPayload = buildNewPoSBlock(minerNode);
    }
    minerNode.verify(blockchain.currentHeight(10));

    final String headHash = latestPayload.get("blockHash").asText();

    // Add the full sync node to the cluster so it peers with minerNode
    cluster.addNode(syncNodeFull);

    // ensure the node reached TTD first
    syncNodeFull.verify(blockchain.minimumHeight(4, 30));

    // A single forkchoiceUpdatedV1 pointing at head kicks off backward sync;
    // syncNodeFull does not have the block yet so it responds SYNCING and begins downloading
    triggerSyncViaForkchoiceUpdate(syncNodeFull, headHash);

    // Wait for full sync to complete and verify the full chain is present
    syncNodeFull.verify(blockchain.minimumHeight(10, 30));

    // Add the snap sync node to the cluster so it peers with minerNode
    cluster.addNode(syncNodeSnap);

    syncNodeSnap.awaitPeerDiscovery(net.awaitPeerCount(2));

    // A single forkchoiceUpdatedV1 pointing at head kicks off snap sync to pivot;
    // syncNodeSnap does not have the block yet so it responds SYNCING and begins downloading
    triggerSyncViaForkchoiceUpdate(syncNodeSnap, headHash);

    // Wait for snap sync to complete and verify the full chain is present
    syncNodeSnap.verify(blockchain.minimumHeight(10, 120));
  }

  /**
   * Drives PoS block building on {@code node} via the Engine API and returns the resulting
   * execution payload.
   *
   * <ol>
   *   <li>engine_forkchoiceUpdatedV1 (with PayloadAttributesV1) → payloadId
   *   <li>engine_getPayloadV1 → executionPayload
   *   <li>engine_newPayloadV1 → VALID
   *   <li>engine_forkchoiceUpdatedV1 (no attributes) → canonical head advanced
   * </ol>
   */
  private ObjectNode buildNewPoSBlock(final BesuNode node)
      throws IOException, InterruptedException {
    final OkHttpClient httpClient = new OkHttpClient();
    final ObjectMapper mapper = new ObjectMapper();

    // Get current head block to derive hash and next timestamp
    final EthBlock.Block currentBlock = node.execute(ethTransactions.block());
    final String headHash = currentBlock.getHash();
    final long nextTimestamp = currentBlock.getTimestamp().longValue() + 1;

    // Step 1: engine_forkchoiceUpdatedV1 with payload attributes to trigger block building
    final String fckWithAttributesRequest =
        "{"
            + "\"jsonrpc\": \"2.0\","
            + "\"method\": \"engine_forkchoiceUpdatedV1\","
            + "\"params\": ["
            + "  {"
            + "    \"headBlockHash\": \""
            + headHash
            + "\","
            + "    \"safeBlockHash\": \""
            + headHash
            + "\","
            + "    \"finalizedBlockHash\": \"0x0000000000000000000000000000000000000000000000000000000000000000\""
            + "  },"
            + "  {"
            + "    \"timestamp\": \"0x"
            + Long.toHexString(nextTimestamp)
            + "\","
            + "    \"prevRandao\": \"0x0000000000000000000000000000000000000000000000000000000000000000\","
            + "    \"suggestedFeeRecipient\": \"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b\""
            + "  }"
            + "],"
            + "\"id\": 67"
            + "}";

    final String payloadId;
    try (final Response response =
        engineCall(httpClient, node, fckWithAttributesRequest).execute()) {
      assertThat(response.code()).isEqualTo(200);
      payloadId = mapper.readTree(response.body().string()).get("result").get("payloadId").asText();
      assertThat(payloadId).isNotEmpty();
    }

    // Wait for block building to complete
    Thread.sleep(500);

    // Step 2: engine_getPayloadV1 to retrieve the built execution payload
    final String getPayloadRequest =
        "{"
            + "\"jsonrpc\": \"2.0\","
            + "\"method\": \"engine_getPayloadV1\","
            + "\"params\": [\""
            + payloadId
            + "\"],"
            + "\"id\": 67"
            + "}";

    final ObjectNode executionPayload;
    final String newBlockHash;
    try (final Response response = engineCall(httpClient, node, getPayloadRequest).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final JsonNode result = mapper.readTree(response.body().string()).get("result");
      executionPayload = (ObjectNode) result;
      newBlockHash = executionPayload.get("blockHash").asText();
      assertThat(newBlockHash).isNotEmpty();
    }

    // Step 3: engine_newPayloadV1 to validate and import the new block
    importPoSBlock(node, executionPayload);

    // Step 4: engine_forkchoiceUpdatedV1 to make the new block the canonical head
    advanceChainHead(node, newBlockHash);

    return executionPayload;
  }

  /**
   * Submits an execution payload to {@code node} via engine_newPayloadV1 and asserts it is VALID.
   * Safe to call even if the node already has the block (e.g. received it via p2p).
   */
  private void importPoSBlock(final BesuNode node, final ObjectNode executionPayload)
      throws IOException {
    final OkHttpClient httpClient = new OkHttpClient();
    final ObjectMapper mapper = new ObjectMapper();

    final String newPayloadRequest =
        "{"
            + "\"jsonrpc\": \"2.0\","
            + "\"method\": \"engine_newPayloadV1\","
            + "\"params\": ["
            + executionPayload
            + "],"
            + "\"id\": 67"
            + "}";

    try (final Response response = engineCall(httpClient, node, newPayloadRequest).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final String status =
          mapper.readTree(response.body().string()).get("result").get("status").asText();
      assertThat(status).isEqualTo("VALID");
    }
  }

  /**
   * Calls engine_forkchoiceUpdatedV1 (without payload attributes) on {@code node} to set {@code
   * blockHash} as the canonical chain head. The node already has the block, so the expected
   * response is VALID.
   */
  private void advanceChainHead(final BesuNode node, final String blockHash) throws IOException {
    assertThat(forkchoiceUpdatedV1Status(node, blockHash)).isEqualTo("VALID");
  }

  /**
   * Calls engine_forkchoiceUpdatedV1 on {@code node} pointing at a block the node does not yet
   * have. The node responds with SYNCING and immediately begins snap-syncing from its peers to
   * reach that block.
   */
  private void triggerSyncViaForkchoiceUpdate(final BesuNode node, final String blockHash)
      throws IOException {
    assertThat(forkchoiceUpdatedV1Status(node, blockHash)).isEqualTo("SYNCING");
  }

  private String forkchoiceUpdatedV1Status(final BesuNode node, final String blockHash)
      throws IOException {
    final OkHttpClient httpClient = new OkHttpClient();
    final ObjectMapper mapper = new ObjectMapper();

    final String fckRequest =
        "{"
            + "\"jsonrpc\": \"2.0\","
            + "\"method\": \"engine_forkchoiceUpdatedV1\","
            + "\"params\": ["
            + "  {"
            + "    \"headBlockHash\": \""
            + blockHash
            + "\","
            + "    \"safeBlockHash\": \""
            + blockHash
            + "\","
            + "    \"finalizedBlockHash\": \"0x0000000000000000000000000000000000000000000000000000000000000000\""
            + "  },"
            + "  null"
            + "],"
            + "\"id\": 67"
            + "}";

    try (final Response response = engineCall(httpClient, node, fckRequest).execute()) {
      assertThat(response.code()).isEqualTo(200);
      return mapper
          .readTree(response.body().string())
          .get("result")
          .get("payloadStatus")
          .get("status")
          .asText();
    }
  }

  private Call engineCall(
      final OkHttpClient httpClient, final BesuNode node, final String request) {
    return httpClient.newCall(
        new Request.Builder()
            .url(node.engineRpcUrl().get())
            .post(RequestBody.create(request, MEDIA_TYPE_JSON))
            .build());
  }

  @Override
  public void tearDownAcceptanceTestBase() {
    cluster.stop();
    super.tearDownAcceptanceTestBase();
  }
}
