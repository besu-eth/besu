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
package org.hyperledger.besu.ethereum.transaction;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.BLOCK_NUMBERS_NOT_ASCENDING;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.INVALID_NONCES;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.INVALID_PRECOMPILE_ADDRESS;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.TIMESTAMPS_NOT_ASCENDING;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.TOO_MANY_BLOCK_CALLS;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BlockSimulationParameter {
  private static final int MAX_BLOCK_CALL_SIZE = 256;
  private static final Address DEFAULT_FROM =
      Address.fromHexString("0x0000000000000000000000000000000000000000");
  static final BlockSimulationParameter EMPTY = new BlockSimulationParameterBuilder().build();

  final List<? extends BlockStateCall> blockStateCalls;
  private final boolean validation;
  private final boolean traceTransfers;
  private final boolean returnFullTransactions;
  private final boolean returnTrieLog;
  private final boolean collectExecutionMetrics;
  private final SECPSignature fakeSignature;

  public BlockSimulationParameter(
      final List<? extends BlockStateCall> blockStateCalls,
      final boolean validation,
      final boolean traceTransfers,
      final boolean returnFullTransactions) {
    this(blockStateCalls, validation, traceTransfers, returnFullTransactions, false, false);
  }

  public BlockSimulationParameter(
      final List<? extends BlockStateCall> blockStateCalls,
      final boolean validation,
      final boolean traceTransfers,
      final boolean returnFullTransactions,
      final boolean returnTrieLog) {
    this(
        blockStateCalls,
        validation,
        traceTransfers,
        returnFullTransactions,
        returnTrieLog,
        false,
        new SECPSignature(BigInteger.ZERO, BigInteger.ZERO, (byte) 0));
  }

  public BlockSimulationParameter(
      final List<? extends BlockStateCall> blockStateCalls,
      final boolean validation,
      final boolean traceTransfers,
      final boolean returnFullTransactions,
      final boolean returnTrieLog,
      final boolean collectExecutionMetrics) {
    this(
        blockStateCalls,
        validation,
        traceTransfers,
        returnFullTransactions,
        returnTrieLog,
        collectExecutionMetrics,
        new SECPSignature(BigInteger.ZERO, BigInteger.ZERO, (byte) 0));
  }

  public BlockSimulationParameter(
      final List<? extends BlockStateCall> blockStateCalls,
      final boolean validation,
      final boolean traceTransfers,
      final boolean returnFullTransactions,
      final boolean returnTrieLog,
      final SECPSignature fakeSignature) {
    this(
        blockStateCalls,
        validation,
        traceTransfers,
        returnFullTransactions,
        returnTrieLog,
        false,
        fakeSignature);
  }

  public BlockSimulationParameter(
      final List<? extends BlockStateCall> blockStateCalls,
      final boolean validation,
      final boolean traceTransfers,
      final boolean returnFullTransactions,
      final boolean returnTrieLog,
      final boolean collectExecutionMetrics,
      final SECPSignature fakeSignature) {
    checkNotNull(blockStateCalls);
    this.blockStateCalls = blockStateCalls;
    this.validation = validation;
    this.traceTransfers = traceTransfers;
    this.returnFullTransactions = returnFullTransactions;
    this.returnTrieLog = returnTrieLog;
    this.collectExecutionMetrics = collectExecutionMetrics;
    this.fakeSignature = fakeSignature;
  }

  public List<? extends BlockStateCall> getBlockStateCalls() {
    return blockStateCalls;
  }

  public boolean isValidation() {
    return validation;
  }

  public boolean isTraceTransfers() {
    return traceTransfers;
  }

  public boolean isReturnFullTransactions() {
    return returnFullTransactions;
  }

  public boolean isReturnTrieLog() {
    return returnTrieLog;
  }

  public boolean isCollectExecutionMetrics() {
    return collectExecutionMetrics;
  }

  public SECPSignature getFakeSignature() {
    return fakeSignature;
  }

  public Optional<BlockStateCallError> validate(final Set<Address> validPrecompileAddresses) {
    if (blockStateCalls.size() > MAX_BLOCK_CALL_SIZE) {
      return Optional.of(TOO_MANY_BLOCK_CALLS);
    }

    Optional<BlockStateCallError> blockNumberError = validateBlockNumbers();
    if (blockNumberError.isPresent()) {
      return blockNumberError;
    }

    Optional<BlockStateCallError> timestampError = validateTimestamps();
    if (timestampError.isPresent()) {
      return timestampError;
    }

    Optional<BlockStateCallError> nonceError = validateNonces();
    if (nonceError.isPresent()) {
      return nonceError;
    }

    return validateStateOverrides(validPrecompileAddresses);
  }

  private Optional<BlockStateCallError> validateBlockNumbers() {
    long previousBlockNumber = -1;
    for (BlockStateCall call : blockStateCalls) {
      Optional<Long> blockNumberOverride = call.getBlockOverrides().getBlockNumber();
      if (blockNumberOverride.isPresent()) {
        long currentBlockNumber = blockNumberOverride.get();
        if (currentBlockNumber <= previousBlockNumber) {
          return Optional.of(BLOCK_NUMBERS_NOT_ASCENDING);
        }
        previousBlockNumber = currentBlockNumber;
      }
    }
    return Optional.empty();
  }

  private Optional<BlockStateCallError> validateTimestamps() {
    long previousTimestamp = -1;
    for (BlockStateCall call : blockStateCalls) {
      Optional<Long> blockTimestampOverride = call.getBlockOverrides().getTimestamp();
      if (blockTimestampOverride.isPresent()) {
        long blockTimestamp = blockTimestampOverride.get();
        if (blockTimestamp <= previousTimestamp) {
          return Optional.of(TIMESTAMPS_NOT_ASCENDING);
        }
        previousTimestamp = blockTimestamp;
      }
    }
    return Optional.empty();
  }

  private Optional<BlockStateCallError> validateNonces() {
    Map<Address, Long> previousNonces = new HashMap<>();
    for (BlockStateCall call : blockStateCalls) {
      for (CallParameter callParameter : call.getCalls()) {
        Address fromAddress = callParameter.getSender().orElse(DEFAULT_FROM);

        if (callParameter.getNonce().isPresent()) {
          long currentNonce = callParameter.getNonce().getAsLong();
          if (previousNonces.containsKey(fromAddress)) {
            long previousNonce = previousNonces.get(fromAddress);
            if (currentNonce <= previousNonce) {
              return Optional.of(INVALID_NONCES);
            }
          }
          previousNonces.put(fromAddress, currentNonce);
        }
      }
    }
    return Optional.empty();
  }

  private Optional<BlockStateCallError> validateStateOverrides(
      final Set<Address> validPrecompileAddresses) {
    for (BlockStateCall call : blockStateCalls) {
      if (call.getStateOverrideMap().isPresent()) {
        var stateOverrideMap = call.getStateOverrideMap().get();
        for (Address stateOverride : stateOverrideMap.keySet()) {
          final StateOverride override = stateOverrideMap.get(stateOverride);
          if (override.getMovePrecompileToAddress().isPresent()) {
            if (!validPrecompileAddresses.contains(stateOverride)) {
              return Optional.of(INVALID_PRECOMPILE_ADDRESS);
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  public static class BlockSimulationParameterBuilder {
    private List<? extends BlockStateCall> blockStateCalls = List.of();
    private boolean validation = false;
    private boolean traceTransfers = false;
    private boolean returnFullTransactions = false;
    private boolean returnTrieLog = false;
    private boolean collectExecutionMetrics = false;
    private SECPSignature fakeSignature =
        new SECPSignature(BigInteger.ZERO, BigInteger.ZERO, (byte) 0);

    public BlockSimulationParameterBuilder blockStateCalls(
        final List<? extends BlockStateCall> blockStateCalls) {
      this.blockStateCalls = blockStateCalls;
      return this;
    }

    public BlockSimulationParameterBuilder validation(final boolean validation) {
      this.validation = validation;
      return this;
    }

    public BlockSimulationParameterBuilder traceTransfers(final boolean traceTransfers) {
      this.traceTransfers = traceTransfers;
      return this;
    }

    public BlockSimulationParameterBuilder returnFullTransactions(
        final boolean returnFullTransactions) {
      this.returnFullTransactions = returnFullTransactions;
      return this;
    }

    public BlockSimulationParameterBuilder returnTrieLog(final boolean returnTrieLog) {
      this.returnTrieLog = returnTrieLog;
      return this;
    }

    public BlockSimulationParameterBuilder collectExecutionMetrics(
        final boolean collectExecutionMetrics) {
      this.collectExecutionMetrics = collectExecutionMetrics;
      return this;
    }

    public BlockSimulationParameterBuilder fakeSignature(final SECPSignature fakeSignature) {
      this.fakeSignature = fakeSignature;
      return this;
    }

    public BlockSimulationParameter build() {
      return new BlockSimulationParameter(
          blockStateCalls,
          validation,
          traceTransfers,
          returnFullTransactions,
          returnTrieLog,
          collectExecutionMetrics,
          fakeSignature);
    }
  }
}
