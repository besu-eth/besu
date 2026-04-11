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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.ethereum.eth.manager.snap.SnapServer.HASH_LAST;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SnapServerTest {
  static Random rand = new Random();

  // Parameterized test to exercise BONSAI and BONSAI_ARCHIVE
  public static Collection<Object[]> flatDbMode() {
    return Arrays.asList(new Object[][] {{FlatDbMode.FULL}, {FlatDbMode.ARCHIVE}});
  }

  record SnapTestAccount(
      Hash addressHash,
      PmtStateTrieAccountValue accountValue,
      MerkleTrie<Bytes32, Bytes> storage,
      Bytes code) {
    Bytes accountRLP() {
      return RLP.encode(accountValue::writeTo);
    }
  }

  static final ObservableMetricsSystem noopMetrics = new NoOpMetricsSystem();
  SegmentedInMemoryKeyValueStorage storage;

  // force a full flat db with code stored by code hash:
  BonsaiWorldStateKeyValueStorage inMemoryStorage;

  WorldStateStorageCoordinator storageCoordinator;
  StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie;
  WorldStateProofProvider proofProvider;

  Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> spyProvider =
      spy(
          new Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>>() {
            // explicit non-final class is necessary for Mockito to spy:
            @Override
            public Optional<BonsaiWorldStateKeyValueStorage> apply(final Hash hash) {
              return Optional.of(inMemoryStorage);
            }
          });

  SnapServer snapServer;

  SnapTestAccount acct1;
  SnapTestAccount acct2;
  SnapTestAccount acct3;
  SnapTestAccount acct4;

  public void setup(final FlatDbMode dbMode) {
    storage = new SegmentedInMemoryKeyValueStorage();

    // force a full flat db with code stored by code hash:
    inMemoryStorage =
        new BonsaiWorldStateKeyValueStorage(
            new BonsaiFlatDbStrategyProvider(
                noopMetrics,
                dbMode == FlatDbMode.FULL
                    ? DataStorageConfiguration.DEFAULT_BONSAI_CONFIG
                    : DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG) {
              @Override
              public FlatDbMode getFlatDbMode() {
                return dbMode;
              }

              @Override
              protected boolean deriveUseCodeStorageByHash(
                  final SegmentedKeyValueStorage composedWorldStateStorage) {
                return true;
              }
            },
            storage,
            new InMemoryKeyValueStorage());

    storageCoordinator = new WorldStateStorageCoordinator(inMemoryStorage);
    storageTrie =
        new StoredMerklePatriciaTrie<>(
            inMemoryStorage::getAccountStateTrieNode, Function.identity(), Function.identity());
    proofProvider = new WorldStateProofProvider(storageCoordinator);

    spyProvider =
        spy(
            new Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>>() {
              // explicit non-final class is necessary for Mockito to spy:
              @Override
              public Optional<BonsaiWorldStateKeyValueStorage> apply(final Hash hash) {
                return Optional.of(inMemoryStorage);
              }
            });

    // start snap server with no Long.MAX_VALUE timeout to avoid flaky tests
    snapServer = new SnapServer(new EthMessages(), storageCoordinator, spyProvider, Long.MAX_VALUE);
    snapServer.start();

    acct1 = createTestAccount("10");
    acct2 = createTestAccount("20");
    acct3 = createTestContractAccount("30", inMemoryStorage);
    acct4 = createTestContractAccount("40", inMemoryStorage);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertNoStartNoOp(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // account found at startHash
    insertTestAccounts(acct4, acct3, acct1, acct2);

    // stop snap server so that we should not be processing snap requests
    snapServer.stop();

    var rangeData = requestAccountRange(acct1.addressHash, acct4.addressHash).accountData(false);

    // assert empty account response and no attempt to fetch worldstate
    assertThat(rangeData.accounts().isEmpty()).isTrue();
    assertThat(rangeData.proofs().isEmpty()).isTrue();
    verify(spyProvider, never()).apply(any());

    // assert empty storage response and no attempt to fetch worldstate
    var storageRange =
        requestStorageRange(
                List.of(Bytes32.wrap(acct3.addressHash.getBytes())), Hash.ZERO, HASH_LAST)
            .slotsData(false);
    assertThat(storageRange.slots().isEmpty()).isTrue();
    assertThat(storageRange.proofs().isEmpty()).isTrue();
    verify(spyProvider, never()).apply(any());

    // assert empty trie nodes response and no attempt to fetch worldstate
    var trieNodes =
        requestTrieNodes(storageTrie.getRootHash(), List.of(List.of(Bytes.fromHexString("0x01"))))
            .nodes(false);
    assertThat(trieNodes.isEmpty()).isTrue();
    verify(spyProvider, never()).apply(any());

    // assert empty code response and no attempt to fetch worldstate
    var codes =
        requestByteCodes(List.of(Bytes32.wrap(acct3.accountValue.getCodeHash().getBytes())))
            .bytecodes(false)
            .codes();
    assertThat(codes.isEmpty()).isTrue();
    verify(spyProvider, never()).apply(any());
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertEmptyRangeLeftProofOfExclusionAndNextAccount(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // for a range request that returns empty, we should return just a proof of exclusion on the
    // left and the next account after the limit hash
    insertTestAccounts(acct1, acct4);

    var rangeData =
        getAndVerifyAccountRangeData(requestAccountRange(acct2.addressHash, acct3.addressHash), 1);

    // expect to find only one value acct4, outside the requested range
    var outOfRangeVal = rangeData.accounts().entrySet().stream().findFirst();
    assertThat(outOfRangeVal).isPresent();
    assertThat(outOfRangeVal.get().getKey()).isEqualTo(acct4.addressHash().getBytes());

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(acct2.addressHash, rangeData)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountLimitRangeResponse(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // assert we limit the range response according to size
    final int acctCount = 2000;
    final long acctRLPSize = 37;

    List<Integer> randomLoad = IntStream.range(1, 4096).boxed().collect(Collectors.toList());
    Collections.shuffle(randomLoad);
    randomLoad.forEach(
        i ->
            insertTestAccounts(
                createTestAccount(
                    Bytes.concatenate(
                            Bytes.fromHexString("0x40"),
                            Bytes.fromHexStringLenient(Integer.toHexString(i * 256)))
                        .toHexString())));

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeBytes(Hash.ZERO.getBytes());
    tmp.writeBytes(HASH_LAST.getBytes());
    tmp.writeBigIntegerScalar(BigInteger.valueOf(acctRLPSize * acctCount));
    tmp.endList();
    var tinyRangeLimit = new GetAccountRangeMessage(tmp.encoded()).wrapMessageData(BigInteger.ONE);

    var rangeData =
        getAndVerifyAccountRangeData(
            (AccountRangeMessage) snapServer.constructGetAccountRangeResponse(tinyRangeLimit),
            acctCount);

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(Hash.ZERO, rangeData)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountLimitRangeResponse_atLeastOneAccount(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    List<Integer> randomLoad = IntStream.range(1, 4096).boxed().collect(Collectors.toList());
    Collections.shuffle(randomLoad);
    randomLoad.forEach(
        i ->
            insertTestAccounts(
                createTestAccount(
                    Bytes.concatenate(
                            Bytes.fromHexString("0x40"),
                            Bytes.fromHexStringLenient(Integer.toHexString(i * 256)))
                        .toHexString())));

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeBytes(Hash.ZERO.getBytes());
    tmp.writeBytes(HASH_LAST.getBytes());
    tmp.writeBigIntegerScalar(BigInteger.ZERO);
    tmp.endList();
    var tinyRangeLimit = new GetAccountRangeMessage(tmp.encoded()).wrapMessageData(BigInteger.ZERO);

    var rangeData =
        getAndVerifyAccountRangeData(
            (AccountRangeMessage) snapServer.constructGetAccountRangeResponse(tinyRangeLimit), 1);

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(Hash.ZERO, rangeData)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertLastEmptyRange(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // When our final range request is empty, no next account is possible,
    //      and we should return just a proof of exclusion of the right
    insertTestAccounts(acct1, acct2);
    var rangeData =
        getAndVerifyAccountRangeData(requestAccountRange(acct3.addressHash, acct4.addressHash), 0);

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(acct3.addressHash, rangeData)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountFoundAtStartHashProof(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // account found at startHash
    insertTestAccounts(acct4, acct3, acct1, acct2);
    var rangeData =
        getAndVerifyAccountRangeData(requestAccountRange(acct1.addressHash, acct4.addressHash), 4);

    // assert proofs are valid for requested range
    assertThat(assertIsValidAccountRangeProof(acct1.addressHash, rangeData)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertCompleteStorageForSingleAccount(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var rangeData =
        requestStorageRange(
            List.of(Bytes32.wrap(acct3.addressHash.getBytes())), Hash.ZERO, HASH_LAST);
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    assertThat(slotsData.slots().size()).isEqualTo(1);
    var firstAccountStorages = slotsData.slots().first();
    assertThat(firstAccountStorages.size()).isEqualTo(10);
    // no proofs for complete storage range:
    assertThat(slotsData.proofs().size()).isEqualTo(0);

    assertThat(
            assertIsValidStorageProof(acct3, Hash.ZERO, firstAccountStorages, slotsData.proofs()))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertPartialStorageForSingleAccountEmptyRange(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct3);
    var rangeData =
        requestStorageRange(
            List.of(Bytes32.wrap(acct3.addressHash.getBytes())),
            Hash.ZERO,
            Hash.fromHexStringLenient("0x00ff"));
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    // expect 1 slot PAST the requested empty range
    assertThat(slotsData.slots().size()).isEqualTo(1);
    // expect left and right proofs for empty storage range:
    assertThat(slotsData.proofs().size()).isGreaterThan(0);
    // assert proofs are valid for the requested range
    assertThat(
            assertIsValidStorageProof(
                acct3, Hash.ZERO, slotsData.slots().first(), slotsData.proofs()))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertPartialStorageLimitHashBetweenSlots(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    Bytes accountShortHash = Bytes.fromHexStringLenient("0x40");
    Hash accountFullHash = Hash.wrap(Bytes32.leftPad(accountShortHash));
    SnapTestAccount testAccount = createTestContractAccount(accountFullHash, 2, inMemoryStorage);

    Hash startHash = Hash.wrap(Bytes32.rightPad(Bytes.fromHexString("12"))); // slot 2
    Hash endHash = Hash.wrap(Bytes32.rightPad(Bytes.fromHexString("13"))); // between slots 2 and 3
    var rangeData =
        requestStorageRange(
            List.of(Bytes32.wrap(testAccount.addressHash.getBytes())), startHash, endHash);

    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    assertThat(slotsData.slots().size()).isEqualTo(1);
    var firstAccountStorages = slotsData.slots().first();
    // expecting to see 2 slots
    assertThat(firstAccountStorages.size()).isEqualTo(2);
    // assert proofs are valid for the requested range
    assertThat(
            assertIsValidStorageProof(
                testAccount, startHash, firstAccountStorages, slotsData.proofs()))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertLastEmptyPartialStorageForSingleAccount(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // When our final range request is empty, no next account is possible,
    //      and we should return just a proof of exclusion of the right

    insertTestAccounts(acct3);
    var rangeData =
        requestStorageRange(
            List.of(Bytes32.wrap(acct3.addressHash.getBytes())), HASH_LAST, HASH_LAST);
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    // expect no slots PAST the requested empty range
    assertThat(slotsData.slots().size()).isEqualTo(0);
    // expect left and right proofs for empty storage range:
    assertThat(slotsData.proofs().size()).isGreaterThan(0);
    // assert proofs are valid for the requested range
    assertThat(
            assertIsValidStorageProof(
                acct3,
                Hash.fromHexStringLenient("0xFF"),
                Collections.emptyNavigableMap(),
                slotsData.proofs()))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageLimitRangeResponse(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    // assert we limit the range response according to bytessize
    final int storageSlotSize = 69;
    final int storageSlotCount = 16;
    insertTestAccounts(acct1, acct2, acct3, acct4);

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(acct3.addressHash, acct4.addressHash),
        (hash, rlpOutput) -> rlpOutput.writeBytes(hash.getBytes()));
    tmp.writeBytes(Hash.ZERO.getBytes());
    tmp.writeBytes(HASH_LAST.getBytes());
    tmp.writeBigIntegerScalar(BigInteger.valueOf(storageSlotCount * storageSlotSize));
    tmp.endList();
    var tinyRangeLimit = new GetStorageRangeMessage(tmp.encoded());

    var rangeData =
        (StorageRangeMessage) snapServer.constructGetStorageRangeResponse(tinyRangeLimit);

    // assert proofs are valid for the requested range
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    assertThat(slotsData.slots().size()).isEqualTo(2);
    var firstAccountStorages = slotsData.slots().first();
    // expecting to see complete 10 slot storage for acct3
    assertThat(firstAccountStorages.size()).isEqualTo(10);
    var secondAccountStorages = slotsData.slots().last();
    // expecting to see only 6 since request was limited to 16 slots
    assertThat(secondAccountStorages.size()).isEqualTo(6);
    // proofs required for interrupted storage range:
    assertThat(slotsData.proofs().size()).isNotEqualTo(0);

    assertThat(
            assertIsValidStorageProof(acct4, Hash.ZERO, secondAccountStorages, slotsData.proofs()))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageLimitRangeResponse_atLeastOneSlot(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(acct3.addressHash, acct4.addressHash),
        (hash, rlpOutput) -> rlpOutput.writeBytes(hash.getBytes()));
    tmp.writeBytes(Hash.ZERO.getBytes());
    tmp.writeBytes(HASH_LAST.getBytes());
    tmp.writeBigIntegerScalar(BigInteger.ZERO);
    tmp.endList();
    var tinyRangeLimit = new GetStorageRangeMessage(tmp.encoded());

    var rangeData =
        (StorageRangeMessage) snapServer.constructGetStorageRangeResponse(tinyRangeLimit);

    // assert proofs are valid for the requested range
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    assertThat(slotsData.slots().size()).isEqualTo(1);
    var firstAccountStorages = slotsData.slots().first();
    // expecting to see complete 10 slot storage for acct3
    assertThat(firstAccountStorages.size()).isEqualTo(1);
    assertThat(slotsData.proofs().size()).isNotEqualTo(0);

    assertThat(
            assertIsValidStorageProof(acct4, Hash.ZERO, firstAccountStorages, slotsData.proofs()))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountTriePathRequest(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var partialPathToAcct2 =
        CompactEncoding.bytesToPath(Bytes32.wrap(acct2.addressHash.getBytes())).slice(0, 1);
    var partialPathToAcct1 = Bytes.fromHexString("0x01"); // first nibble is 1
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(),
            List.of(List.of(partialPathToAcct2), List.of(partialPathToAcct1)));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(2);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountTrieRequest_invalidEmptyPath(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1);
    var partialPathToAcct1 = Bytes.fromHexString("0x01"); // first nibble is 1
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(), List.of(List.of(), List.of(partialPathToAcct1)));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes.isEmpty()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountTrieLimitRequest(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    final int accountNodeSize = 147;
    final int accountNodeLimit = 3;

    var partialPathToAcct1 = Bytes.fromHexString("0x01"); // first nibble is 1
    var partialPathToAcct2 =
        CompactEncoding.bytesToPath(Bytes32.wrap(acct2.addressHash.getBytes())).slice(0, 1);
    var partialPathToAcct3 = Bytes.fromHexString("0x03"); // first nibble is 1
    var partialPathToAcct4 = Bytes.fromHexString("0x04"); // first nibble is 1
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(
            List.of(partialPathToAcct4),
            List.of(partialPathToAcct3),
            List.of(partialPathToAcct2),
            List.of(partialPathToAcct1)),
        (path, rlpOutput) ->
            rlpOutput.writeList(path, (b, subRlpOutput) -> subRlpOutput.writeBytes(b)));
    tmp.writeBigIntegerScalar(BigInteger.valueOf(accountNodeLimit * accountNodeSize));
    tmp.endList();

    var trieNodeRequest =
        (TrieNodesMessage)
            snapServer.constructGetTrieNodesResponse(new GetTrieNodesMessage(tmp.encoded()));

    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    // TODO: adjust this assertion after sorting out the request fudge factor
    assertThat(trieNodes.size()).isEqualTo(accountNodeLimit * 90 / 100);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertAccountTrieLimitRequest_atLeastOneTrieNode(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);

    var partialPathToAcct1 = Bytes.fromHexString("0x01"); // first nibble is 1
    var partialPathToAcct2 =
        CompactEncoding.bytesToPath(Bytes32.wrap(acct2.addressHash.getBytes())).slice(0, 1);
    var partialPathToAcct3 = Bytes.fromHexString("0x03"); // first nibble is 1
    var partialPathToAcct4 = Bytes.fromHexString("0x04"); // first nibble is 1
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(
            List.of(partialPathToAcct4),
            List.of(partialPathToAcct3),
            List.of(partialPathToAcct2),
            List.of(partialPathToAcct1)),
        (path, rlpOutput) ->
            rlpOutput.writeList(path, (b, subRlpOutput) -> subRlpOutput.writeBytes(b)));
    tmp.writeBigIntegerScalar(BigInteger.ZERO);
    tmp.endList();

    var trieNodeRequest =
        (TrieNodesMessage)
            snapServer.constructGetTrieNodesResponse(new GetTrieNodesMessage(tmp.encoded()));

    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageTriePathRequest(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var pathToSlot11 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0101"));
    var pathToSlot12 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0102"));
    var pathToSlot1a = CompactEncoding.encode(Bytes.fromHexStringLenient("0x010A")); // not present
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(),
            List.of(
                List.of(
                    Bytes32.wrap(acct3.addressHash.getBytes()),
                    pathToSlot11,
                    pathToSlot12,
                    pathToSlot1a),
                List.of(
                    Bytes32.wrap(acct4.addressHash.getBytes()),
                    pathToSlot11,
                    pathToSlot12,
                    pathToSlot1a)));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(6);
    assertThat(trieNodes.get(2)).isEqualTo(Bytes.EMPTY);
    assertThat(trieNodes.get(5)).isEqualTo(Bytes.EMPTY);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageTriePathRequest_accountNotPresent(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct4);
    var pathToSlot11 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0101"));
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(),
            List.of(
                List.of(
                    Bytes32.wrap(acct3.addressHash.getBytes()), pathToSlot11) // account not present
                ));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(0);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageTrieShortAccountHashPathRequest(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    Bytes accountShortHash = Bytes.fromHexStringLenient("0x40");
    Hash accountFullHash = Hash.wrap(Bytes32.leftPad(accountShortHash));
    SnapTestAccount testAccount = createTestContractAccount(accountFullHash, 1, inMemoryStorage);
    insertTestAccounts(testAccount);
    var pathToSlot11 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0101"));
    var pathToSlot12 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0102"));
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(),
            List.of(List.of(accountShortHash, pathToSlot11, pathToSlot12)));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(2);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageTrieLimitRequest(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    final int trieNodeSize = 69;
    final int trieNodeLimit = 3;

    var pathToSlot11 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0101"));
    var pathToSlot12 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0102"));
    var pathToSlot1a = CompactEncoding.encode(Bytes.fromHexStringLenient("0x010A")); // not present

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(
            List.of(acct3.addressHash.getBytes(), pathToSlot11, pathToSlot12, pathToSlot1a),
            List.of(acct4.addressHash.getBytes(), pathToSlot11, pathToSlot12, pathToSlot1a)),
        (path, rlpOutput) ->
            rlpOutput.writeList(path, (b, subRlpOutput) -> subRlpOutput.writeBytes(b)));
    tmp.writeBigIntegerScalar(BigInteger.valueOf(trieNodeLimit * trieNodeSize));
    tmp.endList();

    var trieNodeRequest =
        (TrieNodesMessage)
            snapServer.constructGetTrieNodesResponse(new GetTrieNodesMessage(tmp.encoded()));

    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(3);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertStorageTrieLimitRequest_atLeastOneTrieNode(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);

    var pathToSlot11 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0101"));
    var pathToSlot12 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0102"));
    var pathToSlot1a = CompactEncoding.encode(Bytes.fromHexStringLenient("0x010A")); // not present

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(
            List.of(acct3.addressHash.getBytes(), pathToSlot11, pathToSlot12, pathToSlot1a),
            List.of(acct4.addressHash.getBytes(), pathToSlot11, pathToSlot12, pathToSlot1a)),
        (path, rlpOutput) ->
            rlpOutput.writeList(path, (b, subRlpOutput) -> subRlpOutput.writeBytes(b)));
    tmp.writeBigIntegerScalar(BigInteger.ZERO);
    tmp.endList();

    var trieNodeRequest =
        (TrieNodesMessage)
            snapServer.constructGetTrieNodesResponse(new GetTrieNodesMessage(tmp.encoded()));

    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertCodePresent(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var codeRequest =
        requestByteCodes(
            List.of(
                Bytes32.wrap(acct3.accountValue.getCodeHash().getBytes()),
                Bytes32.wrap(acct4.accountValue.getCodeHash().getBytes())));
    assertThat(codeRequest).isNotNull();
    ByteCodesMessage.ByteCodes codes = codeRequest.bytecodes(false);
    assertThat(codes).isNotNull();
    assertThat(codes.codes().size()).isEqualTo(2);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertCodeLimitRequest(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);
    final int codeSize = 32;
    final int codeLimit = 2;

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeList(
        List.of(acct3.accountValue.getCodeHash(), acct4.accountValue.getCodeHash()),
        (hash, rlpOutput) -> rlpOutput.writeBytes(hash.getBytes()));
    tmp.writeBigIntegerScalar(BigInteger.valueOf(codeSize * codeLimit));
    tmp.endList();

    var codeRequest =
        (ByteCodesMessage)
            snapServer.constructGetBytecodesResponse(new GetByteCodesMessage(tmp.encoded()));

    assertThat(codeRequest).isNotNull();
    ByteCodesMessage.ByteCodes codes = codeRequest.bytecodes(false);
    assertThat(codes).isNotNull();
    // TODO adjust this assertion after sorting out the request fudge factor
    assertThat(codes.codes().size()).isEqualTo(codeLimit * 90 / 100);
  }

  @ParameterizedTest
  @MethodSource("flatDbMode")
  public void assertCodeLimitRequest_atLeastOneByteCode(final FlatDbMode flatDbMode) {
    setup(flatDbMode);
    insertTestAccounts(acct1, acct2, acct3, acct4);

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeList(
        List.of(acct3.accountValue.getCodeHash(), acct4.accountValue.getCodeHash()),
        (hash, rlpOutput) -> rlpOutput.writeBytes(hash.getBytes()));
    tmp.writeBigIntegerScalar(BigInteger.ZERO);
    tmp.endList();

    var codeRequest =
        (ByteCodesMessage)
            snapServer.constructGetBytecodesResponse(new GetByteCodesMessage(tmp.encoded()));

    assertThat(codeRequest).isNotNull();
    ByteCodesMessage.ByteCodes codes = codeRequest.bytecodes(false);
    assertThat(codes).isNotNull();
    assertThat(codes.codes().size()).isEqualTo(1);
  }

  static SnapTestAccount createTestAccount(final String hexAddr) {
    return new SnapTestAccount(
        Hash.wrap(Bytes32.rightPad(Bytes.fromHexString(hexAddr))),
        new PmtStateTrieAccountValue(
            rand.nextInt(0, 1), Wei.of(rand.nextLong(0L, 1L)), Hash.EMPTY_TRIE_HASH, Hash.EMPTY),
        new SimpleMerklePatriciaTrie<>(a -> a),
        Bytes.EMPTY);
  }

  static SnapTestAccount createTestContractAccount(
      final String hexAddr, final BonsaiWorldStateKeyValueStorage storage) {
    final Hash acctHash = Hash.wrap(Bytes32.rightPad(Bytes.fromHexString(hexAddr)));
    return createTestContractAccount(acctHash, 1, storage);
  }

  private static void updateStorageArchiveBlock(
      final SegmentedKeyValueStorage storage, final long blockNumber) {
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  static SnapTestAccount createTestContractAccount(
      final Hash acctHash, final int slotKeyGap, final BonsaiWorldStateKeyValueStorage storage) {
    MerkleTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (loc, hash) -> storage.getAccountStorageTrieNode(acctHash, loc, hash),
            Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()),
            a -> a,
            a -> a);
    Bytes32 mockCode = Bytes32.random();

    // mock some storage data
    var flatdb = storage.getFlatDbStrategy();

    // Only Bonsai archive cares about this. Do everything as if we're at
    // block 1 so we know which entry to retrieve from the DB
    updateStorageArchiveBlock(storage.getComposedWorldStateStorage(), 1);

    var updater = storage.updater();
    updater.putCode(Hash.hash(mockCode), mockCode);
    IntStream.iterate(10, i -> i < 20, i -> i + slotKeyGap)
        .boxed()
        .forEach(
            i -> {
              Bytes32 mockBytes32 = Bytes32.rightPad(Bytes.fromHexString(i.toString()));
              var rlpOut = new BytesValueRLPOutput();
              rlpOut.writeBytes(mockBytes32);
              trie.put(mockBytes32, rlpOut.encoded());
              flatdb.putFlatAccountStorageValueByStorageSlotHash(
                  storage.getComposedWorldStateStorage(),
                  updater.getWorldStateTransaction(),
                  acctHash,
                  Hash.wrap(mockBytes32),
                  mockBytes32);
            });
    trie.commit(
        (location, key, value) ->
            updater.putAccountStorageTrieNode(acctHash, location, key, value));
    updater.commit();
    return new SnapTestAccount(
        acctHash,
        new PmtStateTrieAccountValue(
            rand.nextInt(0, 1), Wei.of(rand.nextLong(0L, 1L)),
            Hash.wrap(trie.getRootHash()), Hash.hash(mockCode)),
        trie,
        mockCode);
  }

  void insertTestAccounts(final SnapTestAccount... accounts) {
    final var updater = inMemoryStorage.updater();
    for (SnapTestAccount account : accounts) {
      updater.putAccountInfoState(account.addressHash(), account.accountRLP());
      storageTrie.put(account.addressHash().getBytes(), account.accountRLP());
    }
    storageTrie.commit(updater::putAccountStateTrieNode);
    updater.commit();
    inMemoryStorage
        .getWorldStateBlockNumber()
        .ifPresent(
            currentBlock ->
                updateStorageArchiveBlock(
                    inMemoryStorage.getComposedWorldStateStorage(), currentBlock + 1));
  }

  boolean assertIsValidAccountRangeProof(
      final Hash startHash, final AccountRangeMessage.AccountRangeData accountRange) {
    Bytes32 lastKey =
        Optional.of(accountRange.accounts())
            .filter(z -> !z.isEmpty())
            .map(NavigableMap::lastKey)
            .orElse(Bytes32.wrap(startHash.getBytes()));

    return proofProvider.isValidRangeProof(
        Bytes32.wrap(startHash.getBytes()),
        lastKey,
        storageTrie.getRootHash(),
        accountRange.proofs(),
        accountRange.accounts());
  }

  boolean assertIsValidStorageProof(
      final SnapTestAccount account,
      final Hash startHash,
      final NavigableMap<Bytes32, Bytes> slotRangeData,
      final List<Bytes> proofs) {

    Bytes32 lastKey =
        Optional.of(slotRangeData)
            .filter(z -> !z.isEmpty())
            .map(NavigableMap::lastKey)
            .orElse(Bytes32.wrap(startHash.getBytes()));

    // this is only working for single account ranges for now
    return proofProvider.isValidRangeProof(
        Bytes32.wrap(startHash.getBytes()),
        lastKey,
        Bytes32.wrap(account.accountValue.getStorageRoot().getBytes()),
        proofs,
        slotRangeData);
  }

  AccountRangeMessage requestAccountRange(final Hash startHash, final Hash limitHash) {
    return (AccountRangeMessage)
        snapServer.constructGetAccountRangeResponse(
            GetAccountRangeMessage.create(
                    Hash.wrap(storageTrie.getRootHash()),
                    Bytes32.wrap(startHash.getBytes()),
                    Bytes32.wrap(limitHash.getBytes()))
                .wrapMessageData(BigInteger.ONE));
  }

  StorageRangeMessage requestStorageRange(
      final List<Bytes32> accountHashes, final Hash startHash, final Hash limitHash) {
    return (StorageRangeMessage)
        snapServer.constructGetStorageRangeResponse(
            GetStorageRangeMessage.create(
                    Hash.wrap(storageTrie.getRootHash()),
                    accountHashes,
                    Bytes32.wrap(startHash.getBytes()),
                    Bytes32.wrap(limitHash.getBytes()))
                .wrapMessageData(BigInteger.ONE));
  }

  TrieNodesMessage requestTrieNodes(final Bytes32 rootHash, final List<List<Bytes>> trieNodesList) {
    return (TrieNodesMessage)
        snapServer.constructGetTrieNodesResponse(
            GetTrieNodesMessage.create(Hash.wrap(rootHash), trieNodesList)
                .wrapMessageData(BigInteger.ONE));
  }

  ByteCodesMessage requestByteCodes(final List<Bytes32> codeHashes) {
    return (ByteCodesMessage)
        snapServer.constructGetBytecodesResponse(
            GetByteCodesMessage.create(codeHashes).wrapMessageData(BigInteger.ONE));
  }

  AccountRangeMessage.AccountRangeData getAndVerifyAccountRangeData(
      final AccountRangeMessage range, final int expectedSize) {
    assertThat(range).isNotNull();
    var accountData = range.accountData(false);
    assertThat(accountData).isNotNull();
    assertThat(accountData.accounts().size()).isEqualTo(expectedSize);
    return accountData;
  }
}
