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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainnetBlockBodyValidator implements BlockBodyValidator {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetBlockBodyValidator.class);

  private static final int MAX_OMMERS = 2;

  private static final int MAX_GENERATION = 6;
  protected final ProtocolSchedule protocolSchedule;

  public MainnetBlockBodyValidator(final ProtocolSchedule protocolSchedule) {
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public boolean validateBody(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final Hash worldStateRootHash,
      final HeaderValidationMode ommerValidationMode,
      final BodyValidationMode bodyValidationMode,
      final OptionalLong cumulativeBlockGasUsed) {
    return validateBody(
        context,
        block,
        receipts,
        () -> BodyValidation.receiptsRoot(receipts),
        worldStateRootHash,
        ommerValidationMode,
        bodyValidationMode,
        cumulativeBlockGasUsed);
  }

  @Override
  public boolean validateBody(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final Supplier<Hash> receiptsRoot,
      final Hash worldStateRootHash,
      final HeaderValidationMode ommerValidationMode,
      final BodyValidationMode bodyValidationMode,
      final OptionalLong cumulativeBlockGasUsed) {
    if (bodyValidationMode == BodyValidationMode.NONE) {
      return true;
    }

    if (bodyValidationMode == BodyValidationMode.LIGHT
        || bodyValidationMode == BodyValidationMode.FULL) {
      if (!validateBodyLight(
          context, block, receipts, ommerValidationMode, cumulativeBlockGasUsed)) {
        return false;
      }
    }

    if (bodyValidationMode == BodyValidationMode.ROOT_ONLY
        || bodyValidationMode == BodyValidationMode.FULL) {
      return validateBodyRoots(block, receipts, receiptsRoot, worldStateRootHash);
    }
    return true;
  }

  @VisibleForTesting
  protected boolean validateBodyRoots(
      final Block block,
      final List<TransactionReceipt> receipts,
      final Supplier<Hash> receiptsRootSupplier,
      final Hash worldStateRootHash) {
    final BlockHeader header = block.getHeader();
    final BlockBody body = block.getBody();

    final Bytes32 transactionsRoot = Bytes32.wrap(body.getTransactionsRoot().getBytes());
    if (!validateTransactionsRoot(
        header, Bytes32.wrap(header.getTransactionsRoot().getBytes()), transactionsRoot)) {
      return false;
    }

    final Bytes32 receiptsRoot = Bytes32.wrap(receiptsRootSupplier.get().getBytes());
    if (!validateReceiptsRoot(
        header, Bytes32.wrap(header.getReceiptsRoot().getBytes()), receiptsRoot)) {
      return false;
    }

    if (!validateStateRoot(
        block.getHeader(),
        Bytes32.wrap(block.getHeader().getStateRoot().getBytes()),
        Bytes32.wrap(worldStateRootHash.getBytes()))) {
      LOG.warn("Invalid block RLP : {}", block.toRlp().toHexString());
      receipts.forEach(
          receipt ->
              LOG.warn("Transaction receipt found in the invalid block {}", receipt.toString()));
      return false;
    }
    return true;
  }

  @Override
  public boolean validateBodyLight(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final HeaderValidationMode ommerValidationMode,
      final OptionalLong cumulativeBlockGasUsed) {

    final BlockHeader header = block.getHeader();

    // Use the protocol-specific gas validation strategy
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(header);
    final BlockGasUsedValidator gasValidator = protocolSpec.getBlockGasUsedValidator();
    if (!gasValidator.validate(header, receipts, cumulativeBlockGasUsed)) {
      final long receiptGasUsed =
          receipts.isEmpty() ? 0 : receipts.get(receipts.size() - 1).getCumulativeGasUsed();
      LOG.warn(
          "Invalid block {}: gas used mismatch (header={}, receipts={}, blockGas={})",
          header.toLogString(),
          header.getGasUsed(),
          receiptGasUsed,
          cumulativeBlockGasUsed.orElse(-1));
      return false;
    }

    if (!validateLogsBloom(header, header.getLogsBloom(), BodyValidation.logsBloom(receipts))) {
      return false;
    }

    if (!validateEthHash(context, block, ommerValidationMode)) {
      return false;
    }

    if (!validateWithdrawals(block)) {
      return false;
    }
    return true;
  }

  private boolean validateTransactionsRoot(
      final BlockHeader header, final Bytes32 expected, final Bytes32 actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: transaction root mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private static boolean validateLogsBloom(
      final BlockHeader header, final LogsBloomFilter expected, final LogsBloomFilter actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: logs bloom filter mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private boolean validateReceiptsRoot(
      final BlockHeader header, final Bytes32 expected, final Bytes32 actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: receipts root mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private boolean validateStateRoot(
      final BlockHeader header, final Bytes32 expected, final Bytes32 actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: state root mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private boolean validateEthHash(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode ommerValidationMode) {
    final BlockHeader header = block.getHeader();
    final BlockBody body = block.getBody();

    final Bytes32 ommerHash = Bytes32.wrap(BodyValidation.ommersHash(body.getOmmers()).getBytes());
    if (!validateOmmersHash(header, Bytes32.wrap(header.getOmmersHash().getBytes()), ommerHash)) {
      return false;
    }

    if (!validateOmmers(context, header, body.getOmmers(), ommerValidationMode)) {
      return false;
    }

    return true;
  }

  private static boolean validateOmmersHash(
      final BlockHeader header, final Bytes32 expected, final Bytes32 actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: ommers hash mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private boolean validateOmmers(
      final ProtocolContext context,
      final BlockHeader header,
      final List<BlockHeader> ommers,
      final HeaderValidationMode ommerValidationMode) {
    if (ommers.size() > MAX_OMMERS) {
      LOG.warn(
          "Invalid block {}: ommer count {} exceeds ommer limit {}",
          header.toLogString(),
          ommers.size(),
          MAX_OMMERS);
      return false;
    }

    if (!areOmmersUnique(ommers)) {
      LOG.warn("Invalid block {}: ommers are not unique", header.toLogString());
      return false;
    }

    for (final BlockHeader ommer : ommers) {
      if (!isOmmerValid(context, header, ommer, ommerValidationMode)) {
        LOG.warn("Invalid block {}: ommer is invalid", header.toLogString());
        return false;
      }
    }

    return true;
  }

  private static boolean areOmmersUnique(final List<BlockHeader> ommers) {
    final Set<BlockHeader> uniqueOmmers = new HashSet<>(ommers);

    return uniqueOmmers.size() == ommers.size();
  }

  private static boolean areSiblings(final BlockHeader a, final BlockHeader b) {
    // Siblings cannot be the same.
    if (a.equals(b)) {
      return false;
    }

    return a.getParentHash().equals(b.getParentHash());
  }

  private boolean isOmmerValid(
      final ProtocolContext context,
      final BlockHeader current,
      final BlockHeader ommer,
      final HeaderValidationMode ommerValidationMode) {
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(ommer);
    if (!protocolSpec
        .getOmmerHeaderValidator()
        .validateHeader(ommer, context, ommerValidationMode)) {
      return false;
    }

    if (!ommerValidationMode.isFormOfLightValidation()) {
      return isOmmerSiblingOfAncestor(context, current, ommer);
    } else {
      return true;
    }
  }

  private boolean isOmmerSiblingOfAncestor(
      final ProtocolContext context, final BlockHeader current, final BlockHeader ommer) {
    // The current block is guaranteed to have a parent because it's a valid header.
    final long lastAncestorBlockNumber = Math.max(current.getNumber() - MAX_GENERATION, 0);

    BlockHeader previous = current;
    while (previous.getNumber() > lastAncestorBlockNumber) {
      final BlockHeader ancestor =
          context.getBlockchain().getBlockHeader(previous.getParentHash()).get();
      if (areSiblings(ommer, ancestor)) {
        return true;
      }
      previous = ancestor;
    }
    return false;
  }

  private boolean validateWithdrawals(final Block block) {
    final WithdrawalsValidator withdrawalsValidator =
        protocolSchedule.getByBlockHeader(block.getHeader()).getWithdrawalsValidator();

    if (!withdrawalsValidator.validateWithdrawals(block.getBody().getWithdrawals())) {
      return false;
    }

    if (!withdrawalsValidator.validateWithdrawalsRoot(block)) {
      return false;
    }

    return true;
  }
}
