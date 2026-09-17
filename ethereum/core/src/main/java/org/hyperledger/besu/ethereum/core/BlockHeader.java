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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** A mined Ethereum block header. */
public class BlockHeader extends SealableBlockHeader
    implements org.hyperledger.besu.plugin.data.BlockHeader {

  public static final int MAX_EXTRA_DATA_BYTES = 32;

  public static final long GENESIS_BLOCK_NUMBER = 0L;

  private final long nonce;

  private final Supplier<Hash> hash;

  private final Supplier<ParsedExtraData> parsedExtraData;

  private final Optional<Bytes> rawRlp;

  public BlockHeader(
      final Hash parentHash,
      final Hash ommersHash,
      final Address coinbase,
      final Hash stateRoot,
      final Hash transactionsRoot,
      final Hash receiptsRoot,
      final LogsBloomFilter logsBloom,
      final Difficulty difficulty,
      final long number,
      final long gasLimit,
      final long gasUsed,
      final long timestamp,
      final Bytes extraData,
      final Wei baseFee,
      final Bytes32 mixHashOrPrevRandao,
      final long nonce,
      final Hash withdrawalsRoot,
      final Long blobGasUsed,
      final BlobGas excessBlobGas,
      final Bytes32 parentBeaconBlockRoot,
      final Hash requestsHash,
      final Hash balHash,
      final Long slotNumber,
      final BlockHeaderFunctions blockHeaderFunctions) {
    this(
        parentHash,
        ommersHash,
        coinbase,
        stateRoot,
        transactionsRoot,
        receiptsRoot,
        logsBloom,
        difficulty,
        number,
        gasLimit,
        gasUsed,
        timestamp,
        extraData,
        baseFee,
        mixHashOrPrevRandao,
        nonce,
        withdrawalsRoot,
        blobGasUsed,
        excessBlobGas,
        parentBeaconBlockRoot,
        requestsHash,
        balHash,
        slotNumber,
        blockHeaderFunctions,
        Optional.empty());
  }

  private BlockHeader(
      final Hash parentHash,
      final Hash ommersHash,
      final Address coinbase,
      final Hash stateRoot,
      final Hash transactionsRoot,
      final Hash receiptsRoot,
      final LogsBloomFilter logsBloom,
      final Difficulty difficulty,
      final long number,
      final long gasLimit,
      final long gasUsed,
      final long timestamp,
      final Bytes extraData,
      final Wei baseFee,
      final Bytes32 mixHashOrPrevRandao,
      final long nonce,
      final Hash withdrawalsRoot,
      final Long blobGasUsed,
      final BlobGas excessBlobGas,
      final Bytes32 parentBeaconBlockRoot,
      final Hash requestsHash,
      final Hash balHash,
      final Long slotNumber,
      final BlockHeaderFunctions blockHeaderFunctions,
      final Optional<Bytes> rawRlp) {
    super(
        parentHash,
        ommersHash,
        coinbase,
        stateRoot,
        transactionsRoot,
        receiptsRoot,
        logsBloom,
        difficulty,
        number,
        gasLimit,
        gasUsed,
        timestamp,
        extraData,
        baseFee,
        mixHashOrPrevRandao,
        withdrawalsRoot,
        blobGasUsed,
        excessBlobGas,
        parentBeaconBlockRoot,
        requestsHash,
        balHash,
        slotNumber);
    this.nonce = nonce;
    this.hash = Suppliers.memoize(() -> blockHeaderFunctions.hash(this));
    this.parsedExtraData = Suppliers.memoize(() -> blockHeaderFunctions.parseExtraData(this));
    this.rawRlp = rawRlp;
  }

  private BlockHeader(
      final Hash parentHash,
      final Hash ommersHash,
      final Address coinbase,
      final Hash stateRoot,
      final Hash transactionsRoot,
      final Hash receiptsRoot,
      final LogsBloomFilter logsBloom,
      final Difficulty difficulty,
      final long number,
      final long gasLimit,
      final long gasUsed,
      final long timestamp,
      final Bytes extraData,
      final Wei baseFee,
      final Bytes32 mixHashOrPrevRandao,
      final long nonce,
      final Hash withdrawalsRoot,
      final Long blobGasUsed,
      final BlobGas excessBlobGas,
      final Bytes32 parentBeaconBlockRoot,
      final Hash requestsHash,
      final Hash balHash,
      final Long slotNumber,
      final Hash blockHeaderHash,
      final BlockHeaderFunctions blockHeaderFunctions,
      final Optional<Bytes> rawRlp) {
    super(
        parentHash,
        ommersHash,
        coinbase,
        stateRoot,
        transactionsRoot,
        receiptsRoot,
        logsBloom,
        difficulty,
        number,
        gasLimit,
        gasUsed,
        timestamp,
        extraData,
        baseFee,
        mixHashOrPrevRandao,
        withdrawalsRoot,
        blobGasUsed,
        excessBlobGas,
        parentBeaconBlockRoot,
        requestsHash,
        balHash,
        slotNumber);
    this.nonce = nonce;
    this.hash = Suppliers.memoize(() -> blockHeaderHash);
    this.parsedExtraData = Suppliers.memoize(() -> blockHeaderFunctions.parseExtraData(this));
    this.rawRlp = rawRlp;
  }

  public static boolean hasEmptyBlock(final BlockHeader blockHeader) {
    return blockHeader.getOmmersHash().equals(Hash.EMPTY_LIST_HASH)
        && blockHeader.getTransactionsRoot().equals(Hash.EMPTY_TRIE_HASH)
        && blockHeader
            .getWithdrawalsRoot()
            .map(wsRoot -> wsRoot.equals(Hash.EMPTY_TRIE_HASH))
            .orElse(true)
        && blockHeader
            .getRequestsHash()
            .map(reqHash -> reqHash.equals(Hash.EMPTY_REQUESTS_HASH))
            .orElse(true);
  }

  /**
   * Returns the block mixed hash.
   *
   * @return the block mixed hash
   */
  @Override
  public Hash getMixHash() {
    return Hash.wrap(mixHashOrPrevRandao);
  }

  /**
   * Returns the block nonce.
   *
   * @return the block nonce
   */
  @Override
  public long getNonce() {
    return nonce;
  }

  /**
   * Returns the block extra data field, as parsed by the {@link BlockHeaderFunctions}.
   *
   * @return the block extra data field
   */
  public ParsedExtraData getParsedExtraData() {
    return parsedExtraData.get();
  }

  /**
   * Returns the block header hash.
   *
   * @return the block header hash
   */
  public Hash getHash() {
    return hash.get();
  }

  @Override
  public Hash getBlockHash() {
    return hash.get();
  }

  /**
   * Write an RLP representation.
   *
   * @param out The RLP output to write to
   */
  public void writeTo(final RLPOutput out) {
    rawRlp.ifPresentOrElse(
        out::writeRLPBytes,
        () -> {
          out.startList();

          out.writeBytes(parentHash.getBytes());
          out.writeBytes(ommersHash.getBytes());
          out.writeBytes(coinbase.getBytes());
          out.writeBytes(stateRoot.getBytes());
          out.writeBytes(transactionsRoot.getBytes());
          out.writeBytes(receiptsRoot.getBytes());
          out.writeBytes(logsBloom.getBytes());
          out.writeUInt256Scalar(difficulty);
          out.writeLongScalar(number);
          out.writeLongScalar(gasLimit);
          out.writeLongScalar(gasUsed);
          out.writeLongScalar(timestamp);
          out.writeBytes(extraData);
          out.writeBytes(mixHashOrPrevRandao);
          out.writeLong(nonce);
          do {
            if (maybeBaseFee.isEmpty()) break;
            out.writeUInt256Scalar(maybeBaseFee.get());

            if (withdrawalsRoot == null) break;
            out.writeBytes(withdrawalsRoot.getBytes());

            if (excessBlobGas == null || blobGasUsed == null) break;
            out.writeLongScalar(blobGasUsed);
            out.writeUInt64Scalar(excessBlobGas);

            if (parentBeaconBlockRoot == null) break;
            out.writeBytes(parentBeaconBlockRoot);

            if (requestsHash == null) break;
            out.writeBytes(requestsHash.getBytes());

            if (blockAccessListHash == null) break;
            out.writeBytes(blockAccessListHash.getBytes());

            if (slotNumber == null) break;
            out.writeLongScalar(slotNumber);
          } while (false);
          out.endList();
        });
  }

  public static BlockHeader readFrom(
      final RLPInput input, final BlockHeaderFunctions blockHeaderFunctions) {
    final RLPInput headerRlp = input.readAsRlp();
    if (headerRlp.enterList() == 0) {
      return null;
    }

    final Hash parentHash = Hash.wrap(headerRlp.readBytes32());
    final Hash ommersHash = Hash.wrap(headerRlp.readBytes32());
    final Address coinbase = Address.readFrom(headerRlp);
    final Hash stateRoot = Hash.wrap(headerRlp.readBytes32());
    final Hash transactionsRoot = Hash.wrap(headerRlp.readBytes32());
    final Hash receiptsRoot = Hash.wrap(headerRlp.readBytes32());
    final LogsBloomFilter logsBloom = LogsBloomFilter.readFrom(headerRlp);
    final Difficulty difficulty = Difficulty.of(headerRlp.readUInt256Scalar());
    final long number = headerRlp.readLongScalar();
    final long gasLimit = headerRlp.readLongScalar();
    final long gasUsed = headerRlp.readLongScalar();
    final long timestamp = headerRlp.readLongScalar();
    final Bytes extraData = headerRlp.readBytes();
    final Bytes32 mixHashOrPrevRandao = headerRlp.readBytes32();
    final long nonce = headerRlp.readLong();
    final Wei baseFee =
        !headerRlp.isEndOfCurrentList() ? Wei.of(headerRlp.readUInt256Scalar()) : null;
    final Hash withdrawalHashRoot =
        !(headerRlp.isEndOfCurrentList() || headerRlp.isZeroLengthString())
            ? Hash.wrap(headerRlp.readBytes32())
            : null;
    final Long blobGasUsed = !headerRlp.isEndOfCurrentList() ? headerRlp.readLongScalar() : null;
    final BlobGas excessBlobGas =
        !headerRlp.isEndOfCurrentList() ? BlobGas.of(headerRlp.readUInt64Scalar()) : null;
    final Bytes32 parentBeaconBlockRoot =
        !headerRlp.isEndOfCurrentList() ? headerRlp.readBytes32() : null;
    final Hash requestsHash =
        !headerRlp.isEndOfCurrentList() ? Hash.wrap(headerRlp.readBytes32()) : null;
    final Hash balHash =
        !headerRlp.isEndOfCurrentList() ? Hash.wrap(headerRlp.readBytes32()) : null;
    final Long slotNumber = !headerRlp.isEndOfCurrentList() ? headerRlp.readLongScalar() : null;
    headerRlp.leaveList();
    return new BlockHeader(
        parentHash,
        ommersHash,
        coinbase,
        stateRoot,
        transactionsRoot,
        receiptsRoot,
        logsBloom,
        difficulty,
        number,
        gasLimit,
        gasUsed,
        timestamp,
        extraData,
        baseFee,
        mixHashOrPrevRandao,
        nonce,
        withdrawalHashRoot,
        blobGasUsed,
        excessBlobGas,
        parentBeaconBlockRoot,
        requestsHash,
        balHash,
        slotNumber,
        blockHeaderFunctions,
        Optional.of(headerRlp.raw()));
  }

  public static BlockHeader readFrom(
      final RLPInput input, final BlockHeaderFunctions blockHeaderFunctions, final Hash knownHash) {
    final RLPInput headerRlp = input.readAsRlp();
    if (headerRlp.enterList() == 0) {
      return null;
    }

    final Hash parentHash = Hash.wrap(headerRlp.readBytes32());
    final Hash ommersHash = Hash.wrap(headerRlp.readBytes32());
    final Address coinbase = Address.readFrom(headerRlp);
    final Hash stateRoot = Hash.wrap(headerRlp.readBytes32());
    final Hash transactionsRoot = Hash.wrap(headerRlp.readBytes32());
    final Hash receiptsRoot = Hash.wrap(headerRlp.readBytes32());
    final LogsBloomFilter logsBloom = LogsBloomFilter.readFrom(headerRlp);
    final Difficulty difficulty = Difficulty.of(headerRlp.readUInt256Scalar());
    final long number = headerRlp.readLongScalar();
    final long gasLimit = headerRlp.readLongScalar();
    final long gasUsed = headerRlp.readLongScalar();
    final long timestamp = headerRlp.readLongScalar();
    final Bytes extraData = headerRlp.readBytes();
    final Bytes32 mixHashOrPrevRandao = headerRlp.readBytes32();
    final long nonce = headerRlp.readLong();
    final Wei baseFee =
        !headerRlp.isEndOfCurrentList() ? Wei.of(headerRlp.readUInt256Scalar()) : null;
    final Hash withdrawalHashRoot =
        !(headerRlp.isEndOfCurrentList() || headerRlp.isZeroLengthString())
            ? Hash.wrap(headerRlp.readBytes32())
            : null;
    final Long blobGasUsed = !headerRlp.isEndOfCurrentList() ? headerRlp.readLongScalar() : null;
    final BlobGas excessBlobGas =
        !headerRlp.isEndOfCurrentList() ? BlobGas.of(headerRlp.readUInt64Scalar()) : null;
    final Bytes32 parentBeaconBlockRoot =
        !headerRlp.isEndOfCurrentList() ? headerRlp.readBytes32() : null;
    final Hash requestsHash =
        !headerRlp.isEndOfCurrentList() ? Hash.wrap(headerRlp.readBytes32()) : null;
    final Hash balHash =
        !headerRlp.isEndOfCurrentList() ? Hash.wrap(headerRlp.readBytes32()) : null;
    final Long slotNumber = !headerRlp.isEndOfCurrentList() ? headerRlp.readLongScalar() : null;
    headerRlp.leaveList();
    return new BlockHeader(
        parentHash,
        ommersHash,
        coinbase,
        stateRoot,
        transactionsRoot,
        receiptsRoot,
        logsBloom,
        difficulty,
        number,
        gasLimit,
        gasUsed,
        timestamp,
        extraData,
        baseFee,
        mixHashOrPrevRandao,
        nonce,
        withdrawalHashRoot,
        blobGasUsed,
        excessBlobGas,
        parentBeaconBlockRoot,
        requestsHash,
        balHash,
        slotNumber,
        knownHash,
        blockHeaderFunctions,
        Optional.of(headerRlp.raw()));
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BlockHeader other)) {
      return false;
    }
    return getHash().equals(other.getHash()) && Objects.equals(getBalHash(), other.getBalHash());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getHash(), getBalHash());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("BlockHeader{");
    sb.append("number=").append(number).append(", ");
    sb.append("hash=").append(getHash()).append(", ");
    sb.append("parentHash=").append(parentHash).append(", ");
    sb.append("ommersHash=").append(ommersHash).append(", ");
    sb.append("coinbase=").append(coinbase).append(", ");
    sb.append("stateRoot=").append(stateRoot).append(", ");
    sb.append("transactionsRoot=").append(transactionsRoot).append(", ");
    sb.append("receiptsRoot=").append(receiptsRoot).append(", ");
    sb.append("logsBloom=").append(logsBloom).append(", ");
    sb.append("difficulty=").append(difficulty).append(", ");
    sb.append("gasLimit=").append(gasLimit).append(", ");
    sb.append("gasUsed=").append(gasUsed).append(", ");
    sb.append("timestamp=").append(timestamp).append(", ");
    sb.append("extraData=").append(extraData).append(", ");
    sb.append("baseFee=").append(maybeBaseFee.orElse(null)).append(", ");
    sb.append("mixHashOrPrevRandao=").append(mixHashOrPrevRandao).append(", ");
    sb.append("nonce=").append(nonce).append(", ");
    if (withdrawalsRoot != null) {
      sb.append("withdrawalsRoot=").append(withdrawalsRoot).append(", ");
    }
    if (blobGasUsed != null && excessBlobGas != null) {
      sb.append("blobGasUsed=").append(blobGasUsed).append(", ");
      sb.append("excessBlobGas=").append(excessBlobGas).append(", ");
    }
    if (parentBeaconBlockRoot != null) {
      sb.append("parentBeaconBlockRoot=").append(parentBeaconBlockRoot).append(", ");
    }
    if (requestsHash != null) {
      sb.append("requestsHash=").append(requestsHash).append(", ");
    }
    if (blockAccessListHash != null) {
      sb.append("blockAccessListHash=").append(blockAccessListHash).append(", ");
    }
    if (slotNumber != null) {
      sb.append("slotNumber=").append(slotNumber);
    }
    return sb.append("}").toString();
  }

  public static org.hyperledger.besu.ethereum.core.BlockHeader convertPluginBlockHeader(
      final org.hyperledger.besu.plugin.data.BlockHeader pluginBlockHeader,
      final BlockHeaderFunctions blockHeaderFunctions) {
    return new org.hyperledger.besu.ethereum.core.BlockHeader(
        Hash.fromHexString(pluginBlockHeader.getParentHash().getBytes().toHexString()),
        Hash.fromHexString(pluginBlockHeader.getOmmersHash().getBytes().toHexString()),
        Address.fromHexString(pluginBlockHeader.getCoinbase().getBytes().toHexString()),
        Hash.fromHexString(pluginBlockHeader.getStateRoot().getBytes().toHexString()),
        Hash.fromHexString(pluginBlockHeader.getTransactionsRoot().getBytes().toHexString()),
        Hash.fromHexString(pluginBlockHeader.getReceiptsRoot().getBytes().toHexString()),
        LogsBloomFilter.fromHexString(pluginBlockHeader.getLogsBloom().getBytes().toHexString()),
        Difficulty.fromHexString(pluginBlockHeader.getDifficulty().toHexString()),
        pluginBlockHeader.getNumber(),
        pluginBlockHeader.getGasLimit(),
        pluginBlockHeader.getGasUsed(),
        pluginBlockHeader.getTimestamp(),
        pluginBlockHeader.getExtraData(),
        pluginBlockHeader.getBaseFee().map(Wei::fromQuantity).orElse(null),
        pluginBlockHeader.getPrevRandao().orElse(null),
        pluginBlockHeader.getNonce(),
        pluginBlockHeader
            .getWithdrawalsRoot()
            .map(h -> Hash.fromHexString(h.getBytes().toHexString()))
            .orElse(null),
        pluginBlockHeader.getBlobGasUsed().map(Long::longValue).orElse(null),
        pluginBlockHeader.getExcessBlobGas().map(BlobGas.class::cast).orElse(null),
        pluginBlockHeader.getParentBeaconBlockRoot().orElse(null),
        pluginBlockHeader
            .getRequestsHash()
            .map(h -> Hash.fromHexString(h.getBytes().toHexString()))
            .orElse(null),
        pluginBlockHeader
            .getBalHash()
            .map(h -> Hash.fromHexString(h.getBytes().toHexString()))
            .orElse(null),
        pluginBlockHeader.getOptionalSlotNumber().orElse(null),
        blockHeaderFunctions);
  }

  @Override
  public String toLogString() {
    return getNumber() + " (" + getHash() + ")";
  }
}
