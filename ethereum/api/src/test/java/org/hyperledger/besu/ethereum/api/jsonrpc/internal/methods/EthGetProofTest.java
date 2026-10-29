/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof.GetProofResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof.StorageEntryProof;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EthGetProofTest {

  private static BlockchainQueries blockchainQueries;

  private EthGetProof method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String ETH_METHOD = "eth_getProof";

  private final Address address =
      Address.fromHexString("0x000d836201318ec6899a67540690382780743280");
  private final UInt256 storageKey =
      UInt256.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");

  private static final Address EXISTING_ACCOUNT =
      Address.fromHexString("8bebc8ba651aee624937e7d897853ac30c95a067");

  private final long blockNumber = 500;

  @BeforeAll
  static void setUpClass() {
    final BlockchainSetupUtil blockchainSetupUtil =
        BlockchainSetupUtil.forSnapTesting(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks(
        HeaderValidationMode.LIGHT_DETACHED_ONLY, HeaderValidationMode.LIGHT);
    final MutableBlockchain blockchain = blockchainSetupUtil.getBlockchain();
    final WorldStateArchive worldStateArchive = blockchainSetupUtil.getWorldArchive();
    blockchainQueries =
        new BlockchainQueries(
            blockchainSetupUtil.getProtocolSchedule(),
            blockchain,
            worldStateArchive,
            MiningConfiguration.newDefault());
  }

  @BeforeEach
  void setUp() {
    method = new EthGetProof(blockchainQueries);
  }

  @Test
  void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  void errorWhenNoAddressAccountSupplied() {
    final JsonRpcRequestContext request = requestWithParams(null, null, "latest");

    Assertions.assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid address parameter (index 0)");
  }

  @Test
  void errorWhenNoStorageKeysSupplied() {
    final JsonRpcRequestContext request = requestWithParams(address.toString(), null, "latest");

    Assertions.assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid storage keys parameters (index 1)");
  }

  @Test
  void acceptsMaxStorageKeys() {
    final JsonRpcRequestContext request =
        requestWithParams(address.toString(), storageKeys(EthGetProof.MAX_STORAGE_KEYS), "latest");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(((GetProofResult) ((JsonRpcSuccessResponse) response).getResult()).getStorageProof())
        .hasSize(EthGetProof.MAX_STORAGE_KEYS);
  }

  @Test
  void errorWhenTooManyStorageKeysSupplied() {
    final JsonRpcRequestContext request =
        requestWithParams(
            address.toString(), storageKeys(EthGetProof.MAX_STORAGE_KEYS + 1), "latest");

    Assertions.assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Too many storage keys requested (max 1024, got 1025)")
        .extracting(e -> ((InvalidJsonRpcParameters) e).getRpcErrorType())
        .isEqualTo(RpcErrorType.INVALID_STORAGE_KEYS_PARAMS);
  }

  @Test
  void defaultsToLatestWhenNoBlockSupplied() {
    // Per execution-apis the Block parameter is optional and defaults to 'latest'.
    // Omitting it must behave identically to explicitly passing "latest".
    final JsonRpcRequestContext omitted =
        requestWithParams(address.toString(), new String[] {storageKey.toString()});
    final JsonRpcRequestContext latest =
        requestWithParams(address.toString(), new String[] {storageKey.toString()}, "latest");

    final JsonRpcResponse omittedResponse = method.response(omitted);
    final JsonRpcResponse latestResponse = method.response(latest);

    Assertions.assertThat(omittedResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    Assertions.assertThat(latestResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    Assertions.assertThat(((JsonRpcSuccessResponse) omittedResponse).getResult())
        .usingRecursiveComparison()
        .isEqualTo(((JsonRpcSuccessResponse) latestResponse).getResult());
  }

  @Test
  void shouldReturnNullWhenWorldStateUnavailable() {

    final JsonRpcRequestContext request =
        requestWithParams(
            Address.fromHexString("0x0000000000000000000000000000000000000000"),
            new String[] {storageKey.toString()},
            "0x" + Long.toHexString(blockNumber + 1));

    final JsonRpcResponse response = method.response(request);

    Assertions.assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    Assertions.assertThat(((JsonRpcSuccessResponse) response).getResult()).isNull();
  }

  @Test
  void getProofWithAccount() {
    // Define the address to fetch proof for
    final Address address = Address.fromHexString("8bebc8ba651aee624937e7d897853ac30c95a067");

    final JsonRpcRequestContext request =
        requestWithParams(
            address.toString(),
            new String[] {storageKey.toString()},
            "0x" + Long.toHexString(blockNumber));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    final GetProofResult result = (GetProofResult) response.getResult();

    // Initialize a map to store the decoded nodes (key-value pairs)
    final Map<Bytes32, Bytes> storage = new HashMap<>();

    // Calculate the state root hash from the Merkle Patricia trie proof
    final Bytes32 stateroot =
        Bytes32.wrap(Hash.hash(Bytes.fromHexString(result.getAccountProof().get(0))).getBytes());

    // Decode the account proof nodes and store them in the `storage` map
    result
        .getAccountProof()
        .forEach(
            proof -> {
              final Bytes node = Bytes.fromHexString(proof);
              storage.put(Bytes32.wrap(Hash.hash(node).getBytes()), node);
            });
    result
        .getStorageProof()
        .forEach(
            storageEntry -> {
              storageEntry
                  .getStorageProof()
                  .forEach(
                      proof -> {
                        final Bytes node = Bytes.fromHexString(proof);
                        storage.put(Bytes32.wrap(Hash.hash(node).getBytes()), node);
                      });
            });

    // Create a Merkle Patricia Trie backed by the `proof`
    StoredMerklePatriciaTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (Bytes location, Bytes32 hash) -> Optional.ofNullable(storage.get(hash)),
            stateroot,
            Function.identity(),
            Function.identity());

    // Retrieve the account from the trie and decode it if present
    Optional<PatriciaTrieAccountValue> accountInTrie =
        trie.get(Bytes32.wrap(address.addressHash().getBytes()))
            .map(
                rlp -> {
                  return PatriciaTrieAccountValue.readFrom(RLP.input(rlp));
                });
    final Hash storageRoot =
        Hash.fromHexString("0xbe3d75a1729be157e79c3b77f00206db4d54e3ea14375a015451c88ec067c790");

    // Validate that the account is present in the trie and matches the expected values
    assertThat(accountInTrie).isPresent();
    assertThat(accountInTrie)
        .contains(
            new PatriciaTrieAccountValue(
                1L, // account nonce
                Wei.fromHexString("0x01"), // account balance
                storageRoot,
                Hash.EMPTY // code hash (empty in this case)
                ));
    assertThat(result.getBalance()).isNotNull();
    assertThat(result.getNonce()).isNotNull();
    assertThat(result.getStorageHash()).isNotNull();
    assertThat(result.getCodeHash()).isNotNull();

    // Check the storage
    StoredMerklePatriciaTrie<Bytes, Bytes> trieStorage =
        new StoredMerklePatriciaTrie<>(
            (Bytes location, Bytes32 hash) -> Optional.ofNullable(storage.get(hash)),
            Bytes32.wrap(storageRoot.getBytes()),
            Function.identity(),
            Function.identity());
    // Retrieve the slot from the trie and decode it if present
    Optional<Bytes> slotInTrie = trieStorage.get(Bytes32.wrap(Hash.hash(storageKey).getBytes()));
    assertThat(slotInTrie).isPresent();
    assertThat(slotInTrie).contains(Bytes.fromHexString("0x01"));
  }

  @Test
  void getProofWithoutAccount() {
    // Define the address to fetch proof for (missing account)
    final Address address = Address.fromHexString("7bebc8ba651aee624937e7d897853ac30c95a067");

    final JsonRpcRequestContext request =
        requestWithParams(
            address.toString(),
            new String[] {storageKey.toString()},
            "0x" + Long.toHexString(blockNumber));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    final GetProofResult result = (GetProofResult) response.getResult();
    // Initialize a map to store the decoded nodes (key-value pairs)
    final Map<Bytes32, Bytes> storage = new HashMap<>();

    // Calculate the state root hash from the Merkle Patricia trie proof
    final Bytes32 stateroot =
        Bytes32.wrap(Hash.hash(Bytes.fromHexString(result.getAccountProof().get(0))).getBytes());

    // Decode the account proof nodes and store them in the `storage` map
    result
        .getAccountProof()
        .forEach(
            proof -> {
              final Bytes node = Bytes.fromHexString(proof);
              storage.put(Bytes32.wrap(Hash.hash(node).getBytes()), node);
            });

    // Create a Merkle Patricia Trie backed by the `proof`
    StoredMerklePatriciaTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (Bytes location, Bytes32 hash) -> Optional.ofNullable(storage.get(hash)),
            stateroot,
            Function.identity(),
            Function.identity());

    // Retrieve the account from the trie and decode it if present
    Optional<PatriciaTrieAccountValue> accountInTrie =
        trie.get(Bytes32.wrap(address.addressHash().getBytes()))
            .map(
                rlp -> {
                  return PatriciaTrieAccountValue.readFrom(RLP.input(rlp));
                });
    // Validate that the account is empty
    assertThat(accountInTrie).isEmpty();
  }

  @Test
  void nonExistentAccountStorageProofHasOneEntryPerRequestedKey() {
    final Address address = Address.fromHexString("7bebc8ba651aee624937e7d897853ac30c95a067");
    final UInt256 key1 =
        UInt256.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");
    final UInt256 key2 =
        UInt256.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000002");
    final UInt256 key3 =
        UInt256.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000003");

    final JsonRpcRequestContext request =
        requestWithParams(
            address.toString(),
            new String[] {key1.toString(), key2.toString(), key3.toString()},
            "0x" + Long.toHexString(blockNumber));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    final GetProofResult result = (GetProofResult) response.getResult();

    assertThat(result.getStorageProof())
        .as("storageProof must contain one entry per requested key (EIP-1186)")
        .hasSize(3);

    result
        .getStorageProof()
        .forEach(
            entry -> {
              assertThat(UInt256.fromHexString(entry.getValue()))
                  .as("storage value for non-existent account must be zero")
                  .isEqualTo(UInt256.ZERO);
              assertThat(entry.getStorageProof())
                  .as("proof nodes for non-existent account must be empty")
                  .isEmpty();
            });
  }

  @Test
  void echoesStorageKeysInRequestedFormat() {
    final String key63 = "0x" + "0".repeat(62) + "1";
    final String key64 = "0x" + "0".repeat(63) + "1";
    final GetProofResult result =
        proofResult(EXISTING_ACCOUNT, "0x1", "0x01", "0x", "0x0", "1", key63, key64);

    assertThat(result.getStorageProof())
        .extracting(StorageEntryProof::getKey)
        .containsExactly("0x1", "0x1", "0x0", "0x0", "0x1", key64, key64);
    assertThat(result.getStorageProof())
        .extracting(StorageEntryProof::getValue)
        .containsExactly("0x1", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1");
  }

  @Test
  void returnsOneEntryPerRequestedKeyInRequestOrder() {
    final GetProofResult result = proofResult(EXISTING_ACCOUNT, "0x2", "0x1", "0x2", "0x1");

    assertThat(result.getStorageProof())
        .extracting(StorageEntryProof::getKey)
        .containsExactly("0x2", "0x1", "0x2", "0x1");
    assertThat(result.getStorageProof())
        .extracting(StorageEntryProof::getValue)
        .containsExactly("0x2", "0x1", "0x2", "0x1");
  }

  @Test
  void returnsSameProofForTwoEncodingsOfOneSlot() {
    final String key64 = "0x" + "0".repeat(63) + "1";
    final GetProofResult result = proofResult(EXISTING_ACCOUNT, "0x1", key64);

    final List<StorageEntryProof> entries = result.getStorageProof();
    assertThat(entries).extracting(StorageEntryProof::getKey).containsExactly("0x1", key64);
    assertThat(entries.get(0).getValue()).isEqualTo(entries.get(1).getValue());
    assertThat(entries.get(0).getStorageProof()).isNotEmpty();
    assertThat(entries.get(0).getStorageProof()).isEqualTo(entries.get(1).getStorageProof());
  }

  @Test
  void acceptsManyStorageKeys() {
    final String[] keys =
        IntStream.range(0, 1024)
            .mapToObj(Integer::toHexString)
            .map(k -> "0x" + k)
            .toArray(String[]::new);
    final GetProofResult result = proofResult(EXISTING_ACCOUNT, keys);

    assertThat(result.getStorageProof()).hasSize(1024);
    assertThat(result.getStorageProof().get(1023).getKey()).isEqualTo("0x3ff");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0xzz",
        "zz",
        "0x0x1",
        "0x00000000000000000000000000000000000000000000000000000000000000001"
      })
  void rejectsInvalidStorageKey(final String key) {
    final JsonRpcRequestContext request =
        requestWithParams(EXISTING_ACCOUNT.toString(), new String[] {"0x1", key}, "latest");

    Assertions.assertThatThrownBy(() -> method.response(request))
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e -> {
              assertThat(e.getRpcErrorType()).isEqualTo(RpcErrorType.INVALID_STORAGE_KEYS_PARAMS);
              assertThat(e.getRpcErrorType().getCode()).isEqualTo(-32602);
            });
  }

  @Test
  void absentAccountHasZeroHashes() {
    final GetProofResult result =
        proofResult(Address.fromHexString("7bebc8ba651aee624937e7d897853ac30c95a067"), "0x1");

    assertThat(result.getCodeHash()).isEqualTo(Bytes32.ZERO.toHexString());
    assertThat(result.getStorageHash()).isEqualTo(Bytes32.ZERO.toHexString());
    assertThat(result.getStorageProof()).hasSize(1);
    assertThat(result.getStorageProof().get(0).getValue()).isEqualTo("0x0");
    assertThat(result.getStorageProof().get(0).getStorageProof()).isEmpty();
  }

  @Test
  void returnsPrunedHistoryWhenStateIsNotKept() {
    final BlockHeader header = new BlockHeaderTestFixture().buildHeader();
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(archive.getAccountProof(eq(header), any(), any(), any())).thenReturn(Optional.empty());

    assertThat(errorWithMockedState(header, Optional.of(header), archive, true))
        .isEqualTo(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
    assertThat(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE.getCode()).isEqualTo(4444);
  }

  @Test
  void returnsPrunedHistoryWhenProofIsUnavailable() {
    final BlockHeader header = new BlockHeaderTestFixture().buildHeader();
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(archive.getAccountProof(eq(header), any(), any(), any()))
        .thenAnswer(
            invocation ->
                invocation
                    .<Function<Optional<WorldStateProof>, Optional<?>>>getArgument(3)
                    .apply(Optional.empty()));

    assertThat(errorWithMockedState(header, Optional.of(header), archive, true))
        .isEqualTo(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }

  @Test
  void keepsWorldStateUnavailableWhenHeadStateIsUnavailable() {
    final BlockHeader header = new BlockHeaderTestFixture().buildHeader();
    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(archive.getAccountProof(eq(header), any(), any(), any())).thenReturn(Optional.empty());

    assertThat(errorWithMockedState(header, Optional.of(header), archive, false))
        .isEqualTo(RpcErrorType.WORLD_STATE_UNAVAILABLE);
  }

  @Test
  void keepsWorldStateUnavailableWhenHeaderIsMissing() {
    final BlockHeader header = new BlockHeaderTestFixture().buildHeader();

    assertThat(errorWithMockedState(header, Optional.empty(), mock(WorldStateArchive.class), true))
        .isEqualTo(RpcErrorType.WORLD_STATE_UNAVAILABLE);
  }

  private RpcErrorType errorWithMockedState(
      final BlockHeader header,
      final Optional<BlockHeader> storedHeader,
      final WorldStateArchive archive,
      final boolean headStateAvailable) {
    final BlockchainQueries queries = mock(BlockchainQueries.class);
    final Blockchain blockchain = mock(Blockchain.class);
    final BlockHeader head = new BlockHeaderTestFixture().number(1000).buildHeader();
    when(queries.getBlockchain()).thenReturn(blockchain);
    when(queries.getWorldStateArchive()).thenReturn(archive);
    when(queries.getBlockHeaderByHash(header.getHash())).thenReturn(Optional.of(header));
    when(blockchain.getBlockHeader(header.getHash())).thenReturn(storedHeader);
    when(blockchain.getChainHeadHeader()).thenReturn(head);
    when(archive.isWorldStateAvailable(head.getStateRoot(), head.getHash()))
        .thenReturn(headStateAvailable);

    final JsonRpcResponse response =
        new EthGetProof(queries)
            .response(
                requestWithParams(
                    EXISTING_ACCOUNT.toString(),
                    new String[] {"0x1"},
                    header.getHash().toHexString()));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    return ((JsonRpcErrorResponse) response).getErrorType();
  }

  private GetProofResult proofResult(final Address account, final String... keys) {
    final JsonRpcRequestContext request =
        requestWithParams(account.toString(), keys, "0x" + Long.toHexString(blockNumber));
    return (GetProofResult) ((JsonRpcSuccessResponse) method.response(request)).getResult();
  }

  private static String[] storageKeys(final int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> UInt256.valueOf(i).toHexString())
        .toArray(String[]::new);
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params));
  }
}
