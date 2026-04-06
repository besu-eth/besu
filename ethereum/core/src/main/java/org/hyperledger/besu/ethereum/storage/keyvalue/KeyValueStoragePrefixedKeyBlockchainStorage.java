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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.CHAIN_HEAD_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.FINALIZED_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.FORK_HEADS;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.SAFE_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.SEQ_NO_STORE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.TransactionLocation;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyValueStoragePrefixedKeyBlockchainStorage implements BlockchainStorage {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyValueStoragePrefixedKeyBlockchainStorage.class);

  @Deprecated(since = "23.4.2", forRemoval = true)
  private static final Bytes VARIABLES_PREFIX = Bytes.of(1);

  private static final Bytes BLOCK_HEADER_PREFIX = Bytes.of(2);
  private static final Bytes BLOCK_BODY_PREFIX = Bytes.of(3);
  private static final Bytes TRANSACTION_RECEIPTS_PREFIX = Bytes.of(4);
  private static final Bytes BLOCK_HASH_PREFIX = Bytes.of(5);
  private static final Bytes TOTAL_DIFFICULTY_PREFIX = Bytes.of(6);
  private static final Bytes TRANSACTION_LOCATION_PREFIX = Bytes.of(7);
  private static final Bytes BLOCK_ACCESS_LIST_PREFIX = Bytes.of(8);
  private static final SimpleNoCopyRlpEncoder NO_COPY_RLP_ENCODER = new SimpleNoCopyRlpEncoder();

  final KeyValueStorage blockchainStorage;
  final VariablesStorage variablesStorage;
  final BlockHeaderFunctions blockHeaderFunctions;
  final ProtocolSchedule protocolSchedule;
  final boolean receiptCompaction;

  /**
   * Creates a new blockchain storage instance.
   *
   * @param blockchainStorage the key-value storage for blockchain data
   * @param variablesStorage the storage for blockchain variables
   * @param blockHeaderFunctions functions for block header operations
   * @param protocolSchedule the protocol schedule for fork-aware receipt decoding (nullable for
   *     backward compatibility - if null, uses pre-Amsterdam decoder for all receipts)
   * @param receiptCompaction whether to use receipt compaction
   */
  public KeyValueStoragePrefixedKeyBlockchainStorage(
      final KeyValueStorage blockchainStorage,
      final VariablesStorage variablesStorage,
      final BlockHeaderFunctions blockHeaderFunctions,
      final ProtocolSchedule protocolSchedule,
      final boolean receiptCompaction) {
    this.blockchainStorage = blockchainStorage;
    this.variablesStorage = variablesStorage;
    this.blockHeaderFunctions = blockHeaderFunctions;
    this.protocolSchedule = protocolSchedule;
    this.receiptCompaction = receiptCompaction;
    migrateVariables();
  }

  /**
   * Creates a new blockchain storage instance without protocol schedule. This constructor is
   * provided for backward compatibility with tests that don't need fork-aware receipt decoding.
   * Receipt decoding will use the pre-Amsterdam heuristic-based approach.
   *
   * @param blockchainStorage the key-value storage for blockchain data
   * @param variablesStorage the storage for blockchain variables
   * @param blockHeaderFunctions functions for block header operations
   * @param receiptCompaction whether to use receipt compaction
   */
  public KeyValueStoragePrefixedKeyBlockchainStorage(
      final KeyValueStorage blockchainStorage,
      final VariablesStorage variablesStorage,
      final BlockHeaderFunctions blockHeaderFunctions,
      final boolean receiptCompaction) {
    this(blockchainStorage, variablesStorage, blockHeaderFunctions, null, receiptCompaction);
  }

  @Override
  public Optional<Hash> getChainHead() {
    return variablesStorage.getChainHead();
  }

  @Override
  public Collection<Hash> getForkHeads() {
    return variablesStorage.getForkHeads();
  }

  @Override
  public Optional<Hash> getFinalized() {
    return variablesStorage.getFinalized();
  }

  @Override
  public Optional<Hash> getSafeBlock() {
    return variablesStorage.getSafeBlock();
  }

  @Override
  public Optional<BlockHeader> getBlockHeader(final Hash blockHash) {
    return get(BLOCK_HEADER_PREFIX, blockHash.getBytes())
        .map(b -> BlockHeader.readFrom(RLP.input(b), blockHeaderFunctions, blockHash));
  }

  @Override
  public Optional<BlockBody> getBlockBody(final Hash blockHash) {
    return get(BLOCK_BODY_PREFIX, blockHash.getBytes())
        .map(bytes -> BlockBody.readWrappedBodyFrom(RLP.input(bytes), blockHeaderFunctions));
  }

  @Override
  public Optional<BlockAccessList> getBlockAccessList(final Hash blockHash) {
    return get(BLOCK_ACCESS_LIST_PREFIX, blockHash.getBytes()).map(this::rlpDecodeBlockAccessList);
  }

  @Override
  public Optional<List<TransactionReceipt>> getTransactionReceipts(final Hash blockHash) {
    return get(TRANSACTION_RECEIPTS_PREFIX, blockHash.getBytes())
        .map(this::rlpDecodeTransactionReceipts);
  }

  @Override
  public Optional<Hash> getBlockHash(final long blockNumber) {
    return get(BLOCK_HASH_PREFIX, UInt256.valueOf(blockNumber)).map(this::bytesToHash);
  }

  @Override
  public Optional<Difficulty> getTotalDifficulty(final Hash blockHash) {
    return get(TOTAL_DIFFICULTY_PREFIX, blockHash.getBytes())
        .map(b -> Difficulty.wrap(Bytes32.wrap(b, 0)));
  }

  @Override
  public Optional<TransactionLocation> getTransactionLocation(final Hash transactionHash) {
    return get(TRANSACTION_LOCATION_PREFIX, transactionHash.getBytes())
        .map(bytes -> TransactionLocation.readFrom(RLP.input(bytes)));
  }

  @Override
  public Updater updater() {
    return new Updater(
        blockchainStorage.startTransaction(), variablesStorage.updater(), receiptCompaction);
  }

  private List<TransactionReceipt> rlpDecodeTransactionReceipts(final Bytes bytes) {
    return RLP.input(bytes).readList(in -> TransactionReceiptDecoder.readFrom(in, true));
  }

  private BlockAccessList rlpDecodeBlockAccessList(final Bytes bytes) {
    return BlockAccessListDecoder.decode(RLP.input(bytes));
  }

  private Hash bytesToHash(final Bytes bytes) {
    return Hash.wrap(Bytes32.wrap(bytes, 0));
  }

  Optional<Bytes> get(final Bytes prefix, final Bytes key) {
    return blockchainStorage.get(Bytes.concatenate(prefix, key).toArrayUnsafe()).map(Bytes::wrap);
  }

  /**
   * One time migration of variables from the blockchain storage to the dedicated variable storage.
   * To avoid state inconsistency in case of a downgrade done without running the storage
   * revert-variables subcommand it fails giving the possibility to retry the downgrade procedure.
   */
  private void migrateVariables() {
    final var blockchainUpdater = updater();
    final var variablesUpdater = variablesStorage.updater();

    get(VARIABLES_PREFIX, CHAIN_HEAD_HASH.getBytes())
        .map(this::bytesToHash)
        .ifPresent(
            bch ->
                variablesStorage
                    .getChainHead()
                    .ifPresentOrElse(
                        vch -> {
                          if (!vch.equals(bch)) {
                            logInconsistencyAndFail(CHAIN_HEAD_HASH, bch, vch);
                          }
                        },
                        () -> {
                          variablesUpdater.setChainHead(bch);
                          LOG.info("Migrated key {} to variables storage", CHAIN_HEAD_HASH);
                        }));

    get(VARIABLES_PREFIX, FINALIZED_BLOCK_HASH.getBytes())
        .map(this::bytesToHash)
        .ifPresent(
            bfh -> {
              variablesStorage
                  .getFinalized()
                  .ifPresentOrElse(
                      vfh -> {
                        if (!vfh.equals(bfh)) {
                          logInconsistencyAndFail(FINALIZED_BLOCK_HASH, bfh, vfh);
                        }
                      },
                      () -> {
                        variablesUpdater.setFinalized(bfh);
                        LOG.info("Migrated key {} to variables storage", FINALIZED_BLOCK_HASH);
                      });
            });

    get(VARIABLES_PREFIX, SAFE_BLOCK_HASH.getBytes())
        .map(this::bytesToHash)
        .ifPresent(
            bsh -> {
              variablesStorage
                  .getSafeBlock()
                  .ifPresentOrElse(
                      vsh -> {
                        if (!vsh.equals(bsh)) {
                          logInconsistencyAndFail(SAFE_BLOCK_HASH, bsh, vsh);
                        }
                      },
                      () -> {
                        variablesUpdater.setSafeBlock(bsh);
                        LOG.info("Migrated key {} to variables storage", SAFE_BLOCK_HASH);
                      });
            });

    get(VARIABLES_PREFIX, FORK_HEADS.getBytes())
        .map(bytes -> RLP.input(bytes).readList(in -> this.bytesToHash(in.readBytes32())))
        .ifPresent(
            bfh -> {
              final var vfh = variablesStorage.getForkHeads();
              if (vfh.isEmpty()) {
                variablesUpdater.setForkHeads(bfh);
                LOG.info("Migrated key {} to variables storage", FORK_HEADS);
              } else if (!List.copyOf(vfh).equals(bfh)) {
                logInconsistencyAndFail(FORK_HEADS, bfh, vfh);
              }
            });

    get(Bytes.EMPTY, SEQ_NO_STORE.getBytes())
        .ifPresent(
            bsns -> {
              variablesStorage
                  .getLocalEnrSeqno()
                  .ifPresentOrElse(
                      vsns -> {
                        if (!vsns.equals(bsns)) {
                          logInconsistencyAndFail(SEQ_NO_STORE, bsns, vsns);
                        }
                      },
                      () -> {
                        variablesUpdater.setLocalEnrSeqno(bsns);
                        LOG.info("Migrated key {} to variables storage", SEQ_NO_STORE);
                      });
            });

    blockchainUpdater.removeVariables();

    variablesUpdater.commit();
    blockchainUpdater.commit();
  }

  private static void logInconsistencyAndFail(
      final VariablesStorage.Keys key, final Object bch, final Object vch) {
    LOG.error(
        "Inconsistency found when migrating {} to variables storage,"
            + " probably this is due to a downgrade done without running the `storage revert-variables`"
            + " subcommand first, see https://github.com/hyperledger/besu/pull/5471",
        key);
    throw new IllegalStateException(
        key + " mismatch: blockchain storage value=" + bch + ", variables storage value=" + vch);
  }

  public static class Updater implements BlockchainStorage.Updater {

    private final KeyValueStorageTransaction blockchainTransaction;
    private final VariablesStorage.Updater variablesUpdater;
    private final boolean receiptCompaction;

    Updater(
        final KeyValueStorageTransaction blockchainTransaction,
        final VariablesStorage.Updater variablesUpdater,
        final boolean receiptCompaction) {
      this.blockchainTransaction = blockchainTransaction;
      this.variablesUpdater = variablesUpdater;
      this.receiptCompaction = receiptCompaction;
    }

    @Override
    public void putBlockHeader(final Hash blockHash, final BlockHeader blockHeader) {
      set(BLOCK_HEADER_PREFIX, blockHash.getBytes(), RLP.encode(blockHeader::writeTo));
    }

    @Override
    public void putBlockBody(final Hash blockHash, final BlockBody blockBody) {
      set(BLOCK_BODY_PREFIX, blockHash.getBytes(), RLP.encode(blockBody::writeWrappedBodyTo));
    }

    @Override
    public void putSyncBlockBody(final Hash blockHash, final SyncBlockBody blockBody) {
      set(BLOCK_BODY_PREFIX, blockHash.getBytes(), blockBody.getRlp());
    }

    @Override
    public void putBlockAccessList(final Hash blockHash, final BlockAccessList blockAccessList) {
      set(BLOCK_ACCESS_LIST_PREFIX, blockHash.getBytes(), rlpEncode(blockAccessList));
    }

    @Override
    public void putTransactionLocation(
        final Hash transactionHash, final TransactionLocation transactionLocation) {
      set(
          TRANSACTION_LOCATION_PREFIX,
          transactionHash.getBytes(),
          RLP.encode(transactionLocation::writeTo));
    }

    @Override
    public void putTransactionReceipts(
        final Hash blockHash, final List<TransactionReceipt> transactionReceipts) {
      set(TRANSACTION_RECEIPTS_PREFIX, blockHash.getBytes(), rlpEncode(transactionReceipts));
    }

    @Override
    public void putSyncTransactionReceipts(
        final Hash blockHash, final List<SyncTransactionReceipt> transactionReceipts) {
      set(
          TRANSACTION_RECEIPTS_PREFIX,
          blockHash.getBytes(),
          NO_COPY_RLP_ENCODER.encodeList(
              transactionReceipts.stream()
                  .map(r -> normalizeReceiptRlpElement(r.getRlpBytes()))
                  .toList()));
    }

    @Override
    public void putBlockHash(final long blockNumber, final Hash blockHash) {
      set(BLOCK_HASH_PREFIX, UInt256.valueOf(blockNumber), blockHash.getBytes());
    }

    @Override
    public void putTotalDifficulty(final Hash blockHash, final Difficulty totalDifficulty) {
      set(TOTAL_DIFFICULTY_PREFIX, blockHash.getBytes(), totalDifficulty);
    }

    @Override
    public void setChainHead(final Hash blockHash) {
      variablesUpdater.setChainHead(blockHash);
    }

    @Override
    public void setForkHeads(final Collection<Hash> forkHeadHashes) {
      variablesUpdater.setForkHeads(forkHeadHashes);
    }

    @Override
    public void setFinalized(final Hash blockHash) {
      variablesUpdater.setFinalized(blockHash);
    }

    @Override
    public void setSafeBlock(final Hash blockHash) {
      variablesUpdater.setSafeBlock(blockHash);
    }

    @Override
    public void removeBlockHash(final long blockNumber) {
      remove(BLOCK_HASH_PREFIX, UInt256.valueOf(blockNumber));
    }

    @Override
    public void removeBlockHeader(final Hash blockHash) {
      remove(BLOCK_HEADER_PREFIX, blockHash.getBytes());
    }

    @Override
    public void removeBlockBody(final Hash blockHash) {
      remove(BLOCK_BODY_PREFIX, blockHash.getBytes());
    }

    @Override
    public void removeBlockAccessList(final Hash blockHash) {
      remove(BLOCK_ACCESS_LIST_PREFIX, blockHash.getBytes());
    }

    @Override
    public void removeTransactionReceipts(final Hash blockHash) {
      remove(TRANSACTION_RECEIPTS_PREFIX, blockHash.getBytes());
    }

    @Override
    public void removeTransactionLocation(final Hash transactionHash) {
      remove(TRANSACTION_LOCATION_PREFIX, transactionHash.getBytes());
    }

    @Override
    public void removeTotalDifficulty(final Hash blockHash) {
      remove(TOTAL_DIFFICULTY_PREFIX, blockHash.getBytes());
    }

    @Override
    public void commit() {
      blockchainTransaction.commit();
      variablesUpdater.commit();
    }

    @Override
    public void rollback() {
      variablesUpdater.rollback();
      blockchainTransaction.rollback();
    }

    void set(final Bytes prefix, final Bytes key, final Bytes value) {
      blockchainTransaction.put(
          Bytes.concatenate(prefix, key).toArrayUnsafe(), value.toArrayUnsafe());
    }

    private void remove(final Bytes prefix, final Bytes key) {
      blockchainTransaction.remove(Bytes.concatenate(prefix, key).toArrayUnsafe());
    }

    /**
     * Normalizes a receipt RLP element for storage.
     *
     * <p>EIP-2718 typed receipts received from the network arrive via readBytes() which strips the
     * outer RLP bytes-element wrapper, leaving raw typeCode||rlp_body (first byte in 0x01-0x7f).
     * These must be re-wrapped as a single RLP bytes element so that
     * TransactionReceiptDecoder.decodeTypedReceiptComponents can decode them. Receipts that are
     * already valid RLP elements (first byte >= 0x80) are stored as-is.
     */
    private Bytes normalizeReceiptRlpElement(final Bytes rawBytes) {
      if (rawBytes.isEmpty() || Byte.toUnsignedInt(rawBytes.get(0)) >= 0x80) {
        return rawBytes;
      }
      return NO_COPY_RLP_ENCODER.encode(rawBytes);
    }

    private Bytes rlpEncode(final List<TransactionReceipt> receipts) {
      return RLP.encode(
          o ->
              o.writeList(
                  receipts,
                  (r, rlpOutput) -> {
                    var options =
                        receiptCompaction
                            ? TransactionReceiptEncodingConfiguration.STORAGE_WITH_COMPACTION
                            : TransactionReceiptEncodingConfiguration.STORAGE_WITHOUT_COMPACTION;
                    TransactionReceiptEncoder.writeTo(r, rlpOutput, options);
                  }));
    }

    private Bytes rlpEncode(final BlockAccessList blockAccessList) {
      return RLP.encode(o -> BlockAccessListEncoder.encode(blockAccessList, o));
    }

    private void removeVariables() {
      remove(VARIABLES_PREFIX, CHAIN_HEAD_HASH.getBytes());
      remove(VARIABLES_PREFIX, FINALIZED_BLOCK_HASH.getBytes());
      remove(VARIABLES_PREFIX, SAFE_BLOCK_HASH.getBytes());
      remove(VARIABLES_PREFIX, FORK_HEADS.getBytes());
      remove(Bytes.EMPTY, SEQ_NO_STORE.getBytes());
    }
  }
}
