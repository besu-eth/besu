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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import kotlin.Pair;
import kotlin.collections.ArrayDeque;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See <a href="https://github.com/ethereum/devp2p/blob/master/caps/snap.md">snap</a> */
class SnapServer implements BesuEvents.InitialSyncCompletionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapServer.class);
  private static final int PRIME_STATE_ROOT_CACHE_LIMIT = 128;
  private static final int MAX_ENTRIES_PER_REQUEST = 100000;
  private static final int MAX_RESPONSE_SIZE = 2 * 1024 * 1024;
  private static final int MAX_CODE_LOOKUPS_PER_REQUEST = 1024;
  private static final int MAX_TRIE_LOOKUPS_PER_REQUEST = 1024;
  private static final AccountRangeMessage EMPTY_ACCOUNT_RANGE =
      AccountRangeMessage.create(new HashMap<>(), new ArrayDeque<>());
  private static final StorageRangeMessage EMPTY_STORAGE_RANGE =
      StorageRangeMessage.create(new ArrayDeque<>(), Collections.emptyList());
  private static final TrieNodesMessage EMPTY_TRIE_NODES_MESSAGE =
      TrieNodesMessage.create(new ArrayList<>());
  private static final ByteCodesMessage EMPTY_BYTE_CODES_MESSAGE =
      ByteCodesMessage.create(new ArrayDeque<>());

  static final Hash HASH_LAST = Hash.wrap(Bytes32.leftPad(Bytes.fromHexString("FF"), (byte) 0xFF));

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final EthMessages snapMessages;

  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final Optional<ProtocolContext> protocolContext;

  // whether snap server is enabled
  private final boolean snapServerEnabled;

  // max time per snap request
  private final long maxMillisPerRequest;

  // provide worldstate storage by root hash
  private Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> worldStateStorageProvider =
      __ -> Optional.empty();

  SnapServer(
      final SnapSyncConfiguration snapConfig,
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolContext protocolContext,
      final Synchronizer synchronizer) {
    this.snapServerEnabled =
        Optional.ofNullable(snapConfig)
            .map(SnapSyncConfiguration::isSnapServerEnabled)
            .orElse(false);
    this.snapMessages = snapMessages;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.protocolContext = Optional.of(protocolContext);
    this.maxMillisPerRequest = ResponseSizePredicate.DEFAULT_MAX_MILLIS_PER_REQUEST;
    registerResponseConstructors();

    // subscribe to initial sync completed events to start/stop snap server,
    // not saving the listenerId since we never need to unsubscribe.
    synchronizer.subscribeInitialSync(this);
  }

  /**
   * Create a snap server without registering a listener for worldstate initial sync events or
   * priming worldstates by root hash. Used by unit tests.
   */
  @VisibleForTesting
  SnapServer(
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> worldStateStorageProvider) {
    this(
        snapMessages,
        worldStateStorageCoordinator,
        worldStateStorageProvider,
        ResponseSizePredicate.DEFAULT_MAX_MILLIS_PER_REQUEST);
  }

  @VisibleForTesting
  SnapServer(
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> worldStateStorageProvider,
      final long maxMillisPerRequest) {
    this.snapServerEnabled = true;
    this.snapMessages = snapMessages;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.worldStateStorageProvider = worldStateStorageProvider;
    this.protocolContext = Optional.empty();
    this.maxMillisPerRequest = maxMillisPerRequest;
  }

  @Override
  public void onInitialSyncCompleted() {
    start();
  }

  @Override
  public void onInitialSyncRestart() {
    stop();
  }

  public synchronized SnapServer start() {
    if (!isStarted.get() && snapServerEnabled) {
      // if we are bonsai and full flat, we can provide a worldstate storage:
      var worldStateKeyValueStorage = worldStateStorageCoordinator.worldStateKeyValueStorage();
      if (worldStateKeyValueStorage.getDataStorageFormat().isBonsaiFormat()
          && (worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.FULL)
              || worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.ARCHIVE))) {
        LOGGER.info("Starting SnapServer with Bonsai full flat db");
        var bonsaiArchive =
            protocolContext
                .map(ProtocolContext::getWorldStateArchive)
                .map(BonsaiWorldStateProvider.class::cast);
        var cachedStorageManagerOpt =
            bonsaiArchive.map(archive -> archive.getCachedWorldStorageManager());

        if (cachedStorageManagerOpt.isPresent()) {
          var cachedStorageManager = cachedStorageManagerOpt.get();
          this.worldStateStorageProvider =
              rootHash ->
                  cachedStorageManager
                      .getStorageByRootHash(rootHash)
                      .map(BonsaiWorldStateKeyValueStorage.class::cast);

          // when we start we need to build the cache of latest 128 worldstates
          // trielogs-to-root-hash:
          var blockchain = protocolContext.map(ProtocolContext::getBlockchain).orElse(null);

          // at startup, prime the latest worldstates by roothash:
          cachedStorageManager.primeRootToBlockHashCache(blockchain, PRIME_STATE_ROOT_CACHE_LIMIT);

          var flatDbStrategy =
              ((BonsaiWorldStateKeyValueStorage)
                      worldStateStorageCoordinator.worldStateKeyValueStorage())
                  .getFlatDbStrategy();
          if (!flatDbStrategy.isCodeByCodeHash()) {
            LOGGER.warn("SnapServer requires code stored by codehash, but it is not enabled");
          }
        } else {
          LOGGER.warn(
              "SnapServer started without cached storage manager, this should only happen in tests");
        }
        isStarted.set(true);
      }
    }
    return this;
  }

  public synchronized SnapServer stop() {
    isStarted.set(false);
    return this;
  }

  private void registerResponseConstructors() {
    snapMessages.registerResponseConstructor(
        SnapV1.GET_ACCOUNT_RANGE,
        (peer, messageData, capability) -> constructGetAccountRangeResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_STORAGE_RANGE,
        (peer, messageData, capability) -> constructGetStorageRangeResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_BYTECODES,
        (peer, messageData, capability) -> constructGetBytecodesResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_TRIE_NODES,
        (peer, messageData, capability) -> constructGetTrieNodesResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV2.GET_BLOCK_ACCESS_LISTS,
        (peer, messageData, capability) -> constructGetBlockAccessListsResponse(messageData));
  }

  MessageData constructGetBlockAccessListsResponse(final MessageData message) {
    if (!isStarted.get()) {
      return BlockAccessListsMessage.create(List.of());
    }

    final GetBlockAccessListsMessage getBlockAccessLists =
        GetBlockAccessListsMessage.readFrom(message);
    final Iterable<Hash> blockHashes = getBlockAccessLists.blockHashes(true);

    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();

    final Optional<Blockchain> maybeBlockchain =
        protocolContext.map(ProtocolContext::getBlockchain);

    if (maybeBlockchain.isPresent()) {
      final var blockchain = maybeBlockchain.get();
      int count = 0;
      for (final Hash blockHash : blockHashes) {
        if (count >= MAX_ENTRIES_PER_REQUEST) {
          break;
        }
        count++;

        final Optional<BlockAccessList> maybeBlockAccessList =
            blockchain.getBlockAccessList(blockHash);
        final BytesValueRLPOutput balOutput = new BytesValueRLPOutput();
        if (maybeBlockAccessList.isPresent()) {
          BlockAccessListEncoder.encode(maybeBlockAccessList.get(), balOutput);
        } else {
          // Empty lists are returned for blocks where the BAL is unavailable.
          balOutput.startList();
          balOutput.endList();
        }

        final int encodedSize = balOutput.encodedSize();
        if (responseSizeEstimate + encodedSize > MAX_RESPONSE_SIZE) {
          break;
        }
        responseSizeEstimate += encodedSize;
        rlp.writeRaw(balOutput.encoded());
      }
    }
    rlp.endList();

    return BlockAccessListsMessage.createUnsafe(rlp.encoded());
  }

  MessageData constructGetAccountRangeResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_ACCOUNT_RANGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetAccountRangeMessage getAccountRangeMessage = GetAccountRangeMessage.readFrom(message);
    final GetAccountRangeMessage.Range range = getAccountRangeMessage.range(true);
    final int maxResponseBytes = Math.min(range.responseBytes().intValue(), MAX_RESPONSE_SIZE);

    LOGGER
        .atTrace()
        .setMessage("Received getAccountRangeMessage for {} from {} to {}")
        .addArgument(() -> asLogHash(Bytes32.wrap(range.worldStateRootHash().getBytes())))
        .addArgument(() -> asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())))
        .addArgument(() -> asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())))
        .log();
    try {
      if (range.worldStateRootHash().equals(Hash.EMPTY_TRIE_HASH)) {
        return AccountRangeMessage.create(new HashMap<>(), List.of(MerkleTrie.EMPTY_TRIE_NODE));
      }
      return worldStateStorageProvider
          .apply(range.worldStateRootHash())
          .map(
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                ResponseSizePredicate responseSizePredicate =
                    new ResponseSizePredicate(
                        "account",
                        stopWatch,
                        maxResponseBytes,
                        maxMillisPerRequest,
                        (pair) -> {
                          Bytes bytes =
                              AccountRangeMessage.toSlimAccount(RLP.input(pair.getSecond()));
                          return Bytes32.SIZE + bytes.size();
                        });

                final Bytes32 endKeyBytes = Bytes32.wrap(range.endKeyHash().getBytes());
                var shouldContinuePredicate =
                    new ExceedingPredicate(
                        new EndKeyExceedsPredicate(endKeyBytes).and(responseSizePredicate));

                NavigableMap<Bytes32, Bytes> accounts =
                    storage.streamFlatAccounts(
                        range.startKeyHash().getBytes(), shouldContinuePredicate);

                if (accounts.isEmpty() && shouldContinuePredicate.shouldContinue.get()) {
                  var fromNextHash =
                      range.endKeyHash().compareTo(range.startKeyHash()) >= 0
                          ? range.endKeyHash()
                          : range.startKeyHash();
                  // fetch next account after range, if it exists
                  LOGGER.debug(
                      "found no accounts in range, taking first value starting from {}",
                      asLogHash(Bytes32.wrap(fromNextHash.getBytes())));
                  accounts =
                      storage.streamFlatAccounts(fromNextHash.getBytes(), UInt256.MAX_VALUE, 1L);
                }

                final var worldStateProof =
                    new WorldStateProofProvider(new WorldStateStorageCoordinator(storage));
                final List<Bytes> proof =
                    worldStateProof.getAccountProofRelatedNodes(
                        range.worldStateRootHash(), Bytes32.wrap(range.startKeyHash().getBytes()));

                if (!accounts.isEmpty()) {
                  proof.addAll(
                      worldStateProof.getAccountProofRelatedNodes(
                          range.worldStateRootHash(), accounts.lastKey()));
                }
                var resp = AccountRangeMessage.create(accounts, proof);
                if (accounts.isEmpty()) {
                  LOGGER.debug(
                      "returned empty account range message for {} to  {}, proof count {}",
                      asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())),
                      asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())),
                      proof.size());
                }
                LOGGER.debug(
                    "returned in {} account range {} to {} with {} accounts and {} proofs, resp size {} of max {}",
                    stopWatch,
                    asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())),
                    asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())),
                    accounts.size(),
                    proof.size(),
                    resp.getSize(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty account range due to worldstate not present");
                return EMPTY_ACCOUNT_RANGE;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving account range request", ex);
    }
    return EMPTY_ACCOUNT_RANGE;
  }

  MessageData constructGetStorageRangeResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_STORAGE_RANGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetStorageRangeMessage getStorageRangeMessage = GetStorageRangeMessage.readFrom(message);
    final GetStorageRangeMessage.StorageRange range = getStorageRangeMessage.range(true);
    final int maxResponseBytes = Math.min(range.responseBytes().intValue(), MAX_RESPONSE_SIZE);

    LOGGER
        .atTrace()
        .setMessage("Receive get storage range message size {} from {} to {} for {}")
        .addArgument(message::getSize)
        .addArgument(() -> asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())))
        .addArgument(
            () ->
                Optional.ofNullable(range.endKeyHash())
                    .map(h -> asLogHash(Bytes32.wrap(h.getBytes())))
                    .orElse("''"))
        .addArgument(
            () ->
                range.hashes().stream()
                    .map(SnapServer::asLogHash)
                    .collect(Collectors.joining(",", "[", "]")))
        .log();
    try {
      return worldStateStorageProvider
          .apply(range.worldStateRootHash())
          .map(
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                // reusable predicate to limit by rec count and bytes:
                var responsePredicate =
                    new ResponseSizePredicate(
                        "storage",
                        stopWatch,
                        maxResponseBytes,
                        maxMillisPerRequest,
                        (pair) -> {
                          var slotRlpOutput = new BytesValueRLPOutput();
                          slotRlpOutput.startList();
                          slotRlpOutput.writeBytes(pair.getFirst());
                          slotRlpOutput.writeBytes(pair.getSecond());
                          slotRlpOutput.endList();
                          return slotRlpOutput.encodedSize();
                        });

                // only honor start and end hash if request is for a single account's storage:
                Bytes32 startKeyBytes, endKeyBytes;
                boolean isPartialRange = false;
                if (range.hashes().size() > 1) {
                  startKeyBytes = Bytes32.ZERO;
                  endKeyBytes = Bytes32.wrap(HASH_LAST.getBytes());
                } else {
                  startKeyBytes = Bytes32.wrap(range.startKeyHash().getBytes());
                  endKeyBytes = Bytes32.wrap(range.endKeyHash().getBytes());
                  isPartialRange =
                      !(startKeyBytes.equals(Bytes32.wrap(Hash.ZERO.getBytes()))
                          && endKeyBytes.equals(Bytes32.wrap(HASH_LAST.getBytes())));
                }

                ArrayDeque<NavigableMap<Bytes32, Bytes>> collectedStorages = new ArrayDeque<>();
                List<Bytes> proofNodes = new ArrayList<>();
                final var worldStateProof =
                    new WorldStateProofProvider(new WorldStateStorageCoordinator(storage));

                for (var forAccountHash : range.hashes()) {
                  var predicate =
                      new ExceedingPredicate(
                          new EndKeyExceedsPredicate(endKeyBytes).and(responsePredicate));
                  var accountStorages =
                      storage.streamFlatStorages(
                          Hash.wrap(forAccountHash), startKeyBytes, predicate);

                  //// address partial range queries that return empty
                  if (accountStorages.isEmpty() && isPartialRange) {
                    // fetch next slot after range, if it exists
                    LOGGER.debug(
                        "found no slots in range, taking first value starting from {}",
                        asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())));
                    accountStorages =
                        storage.streamFlatStorages(
                            Hash.wrap(forAccountHash),
                            Bytes32.wrap(range.endKeyHash().getBytes()),
                            UInt256.MAX_VALUE,
                            1L);
                  }

                  // don't send empty storage ranges
                  if (!accountStorages.isEmpty()) {
                    collectedStorages.add(accountStorages);
                  }

                  // if a partial storage range was requested, or we interrupted storage due to
                  // request limits, send proofs:
                  if (isPartialRange || !predicate.shouldGetMore()) {
                    // send a proof for the left side range origin
                    proofNodes.addAll(
                        worldStateProof.getStorageProofRelatedNodes(
                            Bytes32.wrap(getAccountStorageRoot(forAccountHash, storage).getBytes()),
                            forAccountHash,
                            startKeyBytes));
                    if (!accountStorages.isEmpty()) {
                      // send a proof for the last key on the right
                      proofNodes.addAll(
                          worldStateProof.getStorageProofRelatedNodes(
                              Bytes32.wrap(
                                  getAccountStorageRoot(forAccountHash, storage).getBytes()),
                              forAccountHash,
                              accountStorages.lastKey()));
                    }
                  }

                  if (!predicate.shouldGetMore()) {
                    break;
                  }
                }

                var resp = StorageRangeMessage.create(collectedStorages, proofNodes);
                LOGGER.debug(
                    "returned in {} storage {} to {} range {} to {} with {} storages and {} proofs, resp size {} of max {}",
                    stopWatch,
                    asLogHash(range.hashes().first()),
                    asLogHash(range.hashes().last()),
                    asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())),
                    asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())),
                    collectedStorages.size(),
                    proofNodes.size(),
                    resp.getSize(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty storage range due to missing worldstate");
                return EMPTY_STORAGE_RANGE;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving storage range request", ex);
      return EMPTY_STORAGE_RANGE;
    }
  }

  MessageData constructGetBytecodesResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_BYTE_CODES_MESSAGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetByteCodesMessage getByteCodesMessage = GetByteCodesMessage.readFrom(message);
    final GetByteCodesMessage.CodeHashes codeHashes = getByteCodesMessage.codeHashes(true);
    final int maxResponseBytes = Math.min(codeHashes.responseBytes().intValue(), MAX_RESPONSE_SIZE);
    LOGGER
        .atTrace()
        .setMessage("Received get bytecodes message for {} hashes")
        .addArgument(codeHashes.hashes()::size)
        .log();

    try {
      List<Bytes> codeBytes = new ArrayDeque<>();
      var codeHashList =
          (codeHashes.hashes().size() < MAX_CODE_LOOKUPS_PER_REQUEST)
              ? codeHashes.hashes()
              : codeHashes.hashes().subList(0, MAX_CODE_LOOKUPS_PER_REQUEST);
      for (Bytes32 codeHash : codeHashList) {
        if (Hash.EMPTY.getBytes().equals(codeHash)) {
          codeBytes.add(Bytes.EMPTY);
        } else {
          Optional<Bytes> optCode = worldStateStorageCoordinator.getCode(Hash.wrap(codeHash), null);
          if (optCode.isPresent()) {
            if (!codeBytes.isEmpty()
                && (sumListBytes(codeBytes) + optCode.get().size() > maxResponseBytes
                    || stopWatch.getTime() > maxMillisPerRequest)) {
              break;
            }
            codeBytes.add(optCode.get());
          }
        }
      }
      var resp = ByteCodesMessage.create(codeBytes);
      LOGGER.debug(
          "returned in {} code bytes message with {} entries, resp size {} of max {}",
          stopWatch,
          codeBytes.size(),
          resp.getSize(),
          maxResponseBytes);
      return resp;
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving bytecodes request", ex);
      return EMPTY_BYTE_CODES_MESSAGE;
    }
  }

  MessageData constructGetTrieNodesResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_TRIE_NODES_MESSAGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetTrieNodesMessage getTrieNodesMessage = GetTrieNodesMessage.readFrom(message);
    final GetTrieNodesMessage.TrieNodesPaths triePaths = getTrieNodesMessage.paths(true);
    final int maxResponseBytes = Math.min(triePaths.responseBytes().intValue(), MAX_RESPONSE_SIZE);
    LOGGER
        .atTrace()
        .setMessage("Received get trie nodes message of size {}")
        .addArgument(() -> triePaths.paths().size())
        .log();

    try {
      return worldStateStorageProvider
          .apply(triePaths.worldStateRootHash())
          .map(
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                ArrayList<Bytes> trieNodes = new ArrayList<>();
                var triePathList =
                    triePaths.paths().size() < MAX_TRIE_LOOKUPS_PER_REQUEST
                        ? triePaths.paths()
                        : triePaths.paths().subList(0, MAX_TRIE_LOOKUPS_PER_REQUEST);
                for (var triePath : triePathList) {
                  // first element in paths is account
                  if (triePath.size() == 1) {
                    // if there is only one path, presume it should be compact encoded account path
                    final Bytes location = CompactEncoding.decode(triePath.get(0));
                    var optStorage = storage.getTrieNodeUnsafe(location);
                    if (optStorage.isEmpty() && location.isEmpty()) {
                      optStorage = Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
                    }
                    var trieNode = optStorage.orElse(Bytes.EMPTY);
                    if (!trieNodes.isEmpty()
                        && (sumListBytes(trieNodes) + trieNode.size() > maxResponseBytes
                            || stopWatch.getTime(TimeUnit.MILLISECONDS) > maxMillisPerRequest)) {
                      break;
                    }
                    trieNodes.add(trieNode);
                  } else {
                    // There must be at least one element in the path otherwise it is invalid
                    if (triePath.isEmpty()) {
                      LOGGER.debug("returned empty trie nodes message due to invalid path");
                      return EMPTY_TRIE_NODES_MESSAGE;
                    }

                    // otherwise the first element should be account hash, and subsequent paths
                    // are compact encoded account storage paths

                    final Bytes32 accountPrefix = Bytes32.leftPad(triePath.getFirst());
                    var optAccount = storage.getAccount(Hash.wrap(accountPrefix));
                    if (optAccount.isEmpty()) {
                      continue;
                    }

                    List<Bytes> storagePaths = triePath.subList(1, triePath.size());
                    for (var path : storagePaths) {
                      final Bytes location = CompactEncoding.decode(path);
                      var optStorage =
                          storage.getTrieNodeUnsafe(Bytes.concatenate(accountPrefix, location));
                      if (optStorage.isEmpty() && location.isEmpty()) {
                        optStorage = Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
                      }
                      var trieNode = optStorage.orElse(Bytes.EMPTY);
                      if (!trieNodes.isEmpty()
                          && sumListBytes(trieNodes) + trieNode.size() > maxResponseBytes) {
                        break;
                      }
                      trieNodes.add(trieNode);
                    }
                  }
                }
                var resp = TrieNodesMessage.create(trieNodes);
                LOGGER.debug(
                    "returned in {} trie nodes message with {} entries, resp size {} of max {}",
                    stopWatch,
                    trieNodes.size(),
                    resp.getCode(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty trie nodes message due to missing worldstate");
                return EMPTY_TRIE_NODES_MESSAGE;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving trienodes request", ex);
      return EMPTY_TRIE_NODES_MESSAGE;
    }
  }

  /**
   * Predicate that doesn't immediately stop when the delegate predicate returns false, but instead
   * sets a flag to stop after the current element is processed.
   */
  static class ExceedingPredicate implements Predicate<Pair<Bytes32, Bytes>> {
    private final Predicate<Pair<Bytes32, Bytes>> delegate;
    final AtomicBoolean shouldContinue = new AtomicBoolean(true);

    public ExceedingPredicate(final Predicate<Pair<Bytes32, Bytes>> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean test(final Pair<Bytes32, Bytes> pair) {
      final boolean result = delegate.test(pair);
      return shouldContinue.getAndSet(result);
    }

    public boolean shouldGetMore() {
      return shouldContinue.get();
    }
  }

  /** Predicate that stops when the end key is exceeded. */
  record EndKeyExceedsPredicate(Bytes endKey) implements Predicate<Pair<Bytes32, Bytes>> {

    @Override
    public boolean test(final Pair<Bytes32, Bytes> pair) {
      return endKey.compareTo(Bytes.wrap(pair.getFirst())) > 0;
    }
  }

  static class ResponseSizePredicate implements Predicate<Pair<Bytes32, Bytes>> {
    // default to a max of 4 seconds per request
    static final long DEFAULT_MAX_MILLIS_PER_REQUEST = 4000;

    final AtomicInteger byteLimit = new AtomicInteger(0);
    final AtomicInteger recordLimit = new AtomicInteger(0);
    final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    final Function<Pair<Bytes32, Bytes>, Integer> encodingSizeAccumulator;
    final StopWatch stopWatch;
    final int maxResponseBytes;
    final long maxMillisPerRequest;
    final String forWhat;

    ResponseSizePredicate(
        final String forWhat,
        final StopWatch stopWatch,
        final int maxResponseBytes,
        final long maxMillisPerRequest,
        final Function<Pair<Bytes32, Bytes>, Integer> encodingSizeAccumulator) {
      this.stopWatch = stopWatch;
      this.maxResponseBytes = maxResponseBytes;
      this.maxMillisPerRequest = maxMillisPerRequest;
      this.forWhat = forWhat;
      this.encodingSizeAccumulator = encodingSizeAccumulator;
    }

    @Override
    public boolean test(final Pair<Bytes32, Bytes> pair) {
      LOGGER
          .atTrace()
          .setMessage("{} pre-accumulate limits, bytes: {} , stream count: {}")
          .addArgument(() -> forWhat)
          .addArgument(byteLimit::get)
          .addArgument(recordLimit::get)
          .log();
      if (stopWatch.getTime() > maxMillisPerRequest) {
        shouldContinue.set(false);
        LOGGER.warn(
            "{} took too long, stopped at {} ms with {} records and {} bytes",
            forWhat,
            stopWatch.formatTime(),
            recordLimit.get(),
            byteLimit.get());
        return false;
      }

      var underRecordLimit = recordLimit.addAndGet(1) <= MAX_ENTRIES_PER_REQUEST;
      var underByteLimit =
          byteLimit.accumulateAndGet(0, (cur, __) -> cur + encodingSizeAccumulator.apply(pair))
              < maxResponseBytes;
      if (underRecordLimit && underByteLimit) {
        return true;
      } else {
        shouldContinue.set(false);
        LOGGER
            .atDebug()
            .setMessage("{} post-accumulate limits, bytes: {} , stream count: {}")
            .addArgument(() -> forWhat)
            .addArgument(byteLimit::get)
            .addArgument(recordLimit::get)
            .log();
        return false;
      }
    }
  }

  Hash getAccountStorageRoot(
      final Bytes32 accountHash, final BonsaiWorldStateKeyValueStorage storage) {
    return storage
        .getTrieNodeUnsafe(Bytes.concatenate(accountHash, Bytes.EMPTY))
        .map(Hash::hash)
        .orElse(Hash.EMPTY_TRIE_HASH);
  }

  private static int sumListBytes(final List<Bytes> listOfBytes) {
    // TODO: remove hack, 10% is a fudge factor to account for the overhead of rlp encoding
    return listOfBytes.stream().map(Bytes::size).reduce((a, b) -> a + b).orElse(0) * 11 / 10;
  }

  private static String asLogHash(final Bytes32 hash) {
    var str = hash.toHexString();
    return str.substring(0, 4) + ".." + str.substring(59, 63);
  }
}
