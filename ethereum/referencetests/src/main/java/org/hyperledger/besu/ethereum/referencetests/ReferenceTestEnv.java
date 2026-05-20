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
package org.hyperledger.besu.ethereum.referencetests;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.evm.internal.Words.decodeUnsignedLong;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;

/** A memory holder for testing. */
public class ReferenceTestEnv extends BlockHeader {

  public record EnvWithdrawal(
      @JsonProperty("index") String index,
      @JsonProperty("validatorIndex") String validatorIndex,
      @JsonProperty("address") String address,
      @JsonProperty("amount") String amount) {

    Withdrawal asWithdrawal() {
      return new Withdrawal(
          UInt64.fromHexString(index),
          UInt64.fromHexString(validatorIndex),
          Address.fromHexString(address),
          GWei.fromHexString(amount));
    }
  }

  private final String parentDifficulty;

  private final String parentBaseFee;

  private final String parentGasUsed;

  private final String parentGasLimit;

  private final String parentTimestamp;

  private final List<Withdrawal> withdrawals;

  private final Map<Long, Hash> blockHashes;

  private final String parentExcessBlobGas;

  private final String parentBlobGasUsed;

  private final Bytes32 beaconRoot;

  private final boolean isStateTest;

  /**
   * Public constructor.
   *
   * @param coinbase Coinbase/beneficiary for the mock block being tested.
   * @param difficulty Difficulty for the mock block being tested.
   * @param gasLimit Gas Limit for the mock block being tested.
   * @param number Block number for the mock block being tested.
   * @param baseFee Optional BaseFee for the mock block being tested.
   * @param timestamp Timestamp for the mock block being tested.
   * @param random Optional RANDAO or the mock block being tested.
   */
  @JsonCreator
  public ReferenceTestEnv(
      @JsonProperty("parentBeaconBlockRoot") final String beaconRoot,
      @JsonProperty("blockHashes") final Map<String, String> blockHashes,
      @JsonProperty("ommers") final List<String> _ommers,
      @JsonProperty("previousHash") final String previousHash,
      @JsonProperty("withdrawals") final List<EnvWithdrawal> withdrawals,
      @JsonProperty("currentBaseFee") final String baseFee,
      @JsonProperty("currentBeaconRoot") final String currentBeaconRoot,
      @JsonProperty("currentBlobGasUsed") final String currentBlobGasUsed,
      @JsonProperty("currentCoinbase") final String coinbase,
      @JsonProperty("currentDifficulty") final String difficulty,
      @JsonProperty("currentExcessBlobGas") final String currentExcessBlobGas,
      @JsonProperty("currentGasLimit") final String gasLimit,
      @JsonProperty("currentNumber") final String number,
      @JsonProperty("currentRandom") final String random,
      @JsonProperty("currentStateRoot") final String stateRoot,
      @JsonProperty("currentTimestamp") final String timestamp,
      @JsonProperty("currentWithdrawalsRoot") final String currentWithdrawalsRoot,
      @JsonProperty("parentBaseFee") final String parentBaseFee,
      @JsonProperty("parentBlobGasUsed") final String parentBlobGasUsed,
      @JsonProperty("parentDifficulty") final String parentDifficulty,
      @JsonProperty("parentExcessBlobGas") final String parentExcessBlobGas,
      @JsonProperty("parentGasLimit") final String parentGasLimit,
      @JsonProperty("parentGasUsed") final String parentGasUsed,
      @JsonProperty("parentTimestamp") final String parentTimestamp,
      @JsonProperty("parentUncleHash") final String _parentUncleHash,
      @JsonProperty("isStateTest") final String isStateTest,
      @JsonProperty("slotNumber") final String slotNumber) {
    super(
        generateTestBlockHash(previousHash, number),
        Hash.EMPTY_LIST_HASH, // ommersHash
        Address.fromHexString(coinbase),
        Optional.ofNullable(stateRoot).map(Hash::fromHexString).orElse(Hash.EMPTY), // stateRoot
        Hash.EMPTY, // transactionsRoot
        Hash.EMPTY, // receiptsRoot
        new LogsBloomFilter(),
        difficulty == null ? null : Difficulty.fromHexOrDecimalString(difficulty),
        number == null ? 0 : Long.decode(number),
        gasLimit == null ? 15_000_000L : Long.decode(gasLimit),
        0L,
        timestamp == null ? 0L : decodeUnsignedLong(timestamp),
        Bytes.EMPTY,
        Optional.ofNullable(baseFee).map(Wei::fromHexString).orElse(null),
        Optional.ofNullable(random).map(Difficulty::fromHexString).orElse(Difficulty.ZERO),
        0L,
        currentWithdrawalsRoot == null ? null : Hash.fromHexString(currentWithdrawalsRoot),
        currentBlobGasUsed == null ? null : Long.decode(currentBlobGasUsed),
        currentExcessBlobGas == null ? null : BlobGas.of(Long.decode(currentExcessBlobGas)),
        beaconRoot == null ? null : Bytes32.fromHexString(beaconRoot),
        null, // requestsHash
        null, // block access list hash
        slotNumber == null ? null : decodeUnsignedLong(slotNumber),
        new MainnetBlockHeaderFunctions());
    this.parentDifficulty = parentDifficulty;
    this.parentBaseFee = parentBaseFee;
    this.parentGasUsed = parentGasUsed;
    this.parentGasLimit = parentGasLimit;
    this.parentTimestamp = parentTimestamp;
    this.parentExcessBlobGas = parentExcessBlobGas;
    this.parentBlobGasUsed = parentBlobGasUsed;
    this.withdrawals =
        withdrawals == null
            ? List.of()
            : withdrawals.stream().map(EnvWithdrawal::asWithdrawal).toList();
    this.blockHashes =
        blockHashes == null
            ? Map.of()
            : blockHashes.entrySet().stream()
                .map(
                    entry ->
                        Map.entry(
                            Long.decode(entry.getKey()), Hash.fromHexString(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (beaconRoot == null) {
      if (currentBeaconRoot == null) {
        this.beaconRoot = null;
      } else {
        this.beaconRoot = Bytes32.wrap(Hash.fromHexString(currentBeaconRoot).getBytes());
      }
    } else {
      this.beaconRoot = Bytes32.wrap(Hash.fromHexString(beaconRoot).getBytes());
    }
    this.isStateTest = Boolean.parseBoolean(isStateTest);
  }

  @Override
  public Difficulty getDifficulty() {
    return difficulty == null ? Difficulty.ZERO : super.getDifficulty();
  }

  private static Hash generateTestBlockHash(final String previousHash, final String number) {
    if (Strings.isNullOrEmpty(previousHash)) {
      if (number == null) {
        return Hash.EMPTY;
      } else {
        final byte[] bytes = Long.toString(Long.decode(number) - 1).getBytes(UTF_8);
        return Hash.hash(Bytes.wrap(bytes));
      }
    } else {
      return Hash.wrap(Bytes32.fromHexString(previousHash));
    }
  }

  public BlockHeader parentBlockHeader(final ProtocolSpec protocolSpec) {
    var builder =
        BlockHeaderBuilder.fromHeader(this)
            .blockHeaderFunctions(protocolSpec.getBlockHeaderFunctions());
    if (protocolSpec.getWithdrawalsProcessor().isPresent()) {
      builder.withdrawalsRoot(BodyValidation.withdrawalsRoot(withdrawals));
    }
    if ((maybeBaseFee.isEmpty() || maybeBaseFee.get().isEmpty())
        && protocolSpec.getFeeMarket().implementsBaseFee()) {
      builder.baseFee(
          ((BaseFeeMarket) protocolSpec.getFeeMarket())
              .computeBaseFee(
                  number,
                  Wei.fromHexString(parentBaseFee),
                  Long.decode(parentGasUsed),
                  gasLimit / 2));
    }
    if (difficulty == null && parentDifficulty != null) {
      builder.difficulty(
          Difficulty.of(
              protocolSpec
                  .getDifficultyCalculator()
                  .nextDifficulty(
                      timestamp,
                      BlockHeaderBuilder.createDefault()
                          .difficulty(Difficulty.fromHexOrDecimalString(parentDifficulty))
                          .number(number - 1)
                          .buildBlockHeader())));
    }
    if (parentExcessBlobGas != null && parentBlobGasUsed != null) {
      builder.excessBlobGas(BlobGas.of(Long.decode(parentExcessBlobGas)));
      builder.blobGasUsed(Long.decode(parentBlobGasUsed));
    }
    Hash grandParentHash = blockHashes.get(number - 2);
    builder.parentHash(grandParentHash == null ? Hash.ZERO : grandParentHash);

    return builder.buildBlockHeader();
  }

  public List<Withdrawal> getWithdrawals() {
    return withdrawals;
  }

  public Optional<Hash> getBlockhashByNumber(final long number) {
    return Optional.ofNullable(blockHashes.get(number));
  }

  public Map<Long, Hash> getBlockHashes() {
    return blockHashes;
  }

  public String getParentExcessBlobGas() {
    return parentExcessBlobGas;
  }

  public boolean isStateTest() {
    return isStateTest;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof ReferenceTestEnv that)) return false;
    if (!super.equals(o)) return false;
    return Objects.equals(parentDifficulty, that.parentDifficulty)
        && Objects.equals(parentBaseFee, that.parentBaseFee)
        && Objects.equals(parentGasUsed, that.parentGasUsed)
        && Objects.equals(parentGasLimit, that.parentGasLimit)
        && Objects.equals(parentTimestamp, that.parentTimestamp)
        && Objects.equals(parentBlobGasUsed, that.parentBlobGasUsed)
        && Objects.equals(parentExcessBlobGas, that.parentExcessBlobGas)
        && Objects.equals(withdrawals, that.withdrawals)
        && Objects.equals(beaconRoot, that.beaconRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        parentDifficulty,
        parentBaseFee,
        parentGasUsed,
        parentGasLimit,
        parentTimestamp,
        parentBlobGasUsed,
        parentExcessBlobGas,
        withdrawals,
        beaconRoot);
  }
}
