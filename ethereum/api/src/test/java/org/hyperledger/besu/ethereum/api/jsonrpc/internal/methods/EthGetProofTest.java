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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof.GetProofResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EthGetProofTest {

  private static BlockchainQueries blockchainQueries;

  private EthGetProof method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String ETH_METHOD = "eth_getProof";

  private final Address address =
      Address.fromHexString("0x000d836201318ec6899a67540690382780743280");
  private final UInt256 storageKey =
      UInt256.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");

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
  void errorWhenNoBlockNumberSupplied() {
    final JsonRpcRequestContext request = requestWithParams(address.toString(), new String[] {});

    Assertions.assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid block or block hash parameter");
  }

  @Test
  void shouldReturnNullWhenWorldStateUnavailable() {

    final JsonRpcRequestContext request =
        requestWithParams(
            Address.fromHexString("0x0000000000000000000000000000000000000000"),
            new String[] {storageKey.toString()},
            String.valueOf(501));

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
            address.toString(), new String[] {storageKey.toString()}, String.valueOf(blockNumber));

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
    Optional<PmtStateTrieAccountValue> accountInTrie =
        trie.get(Bytes32.wrap(address.addressHash().getBytes()))
            .map(
                rlp -> {
                  return PmtStateTrieAccountValue.readFrom(RLP.input(rlp));
                });
    final Hash storageRoot =
        Hash.fromHexString("0xbe3d75a1729be157e79c3b77f00206db4d54e3ea14375a015451c88ec067c790");

    // Validate that the account is present in the trie and matches the expected values
    assertThat(accountInTrie).isPresent();
    assertThat(accountInTrie)
        .contains(
            new PmtStateTrieAccountValue(
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
            address.toString(), new String[] {storageKey.toString()}, String.valueOf(blockNumber));

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
    Optional<PmtStateTrieAccountValue> accountInTrie =
        trie.get(Bytes32.wrap(address.addressHash().getBytes()))
            .map(
                rlp -> {
                  return PmtStateTrieAccountValue.readFrom(RLP.input(rlp));
                });
    // Validate that the account is empty
    assertThat(accountInTrie).isEmpty();
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params));
  }
}
