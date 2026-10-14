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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.FrameTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.FrameTransactionProcessor.ValidationPrefixResult;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides whether a frame transaction is admissible to the EIP-8141 public mempool at a given head:
 * its validation prefix must be one of the recognized shapes, and simulating that prefix against
 * the head state must approve payment without breaking the validation trace rules.
 */
public class FrameTransactionAdmission {

  private static final Logger LOG = LoggerFactory.getLogger(FrameTransactionAdmission.class);

  private final ProtocolSchedule protocolSchedule;
  private final WorldStateArchive worldStateArchive;

  /**
   * An admissible transaction, with what the pool's bookkeeping needs.
   *
   * @param prefix the recognized validation prefix
   * @param payer the account that approved payment
   * @param maxCost the most the payer can be charged
   * @param payerBalance the payer's balance at the head
   * @param codedPaymaster whether payment comes from a third party running code, which the public
   *     mempool limits to one pending transaction at a time
   */
  public record Admitted(
      ValidationPrefix prefix,
      Address payer,
      Wei maxCost,
      Wei payerBalance,
      boolean codedPaymaster) {}

  /**
   * The admission decision.
   *
   * @param admitted the admission, when the transaction is admissible
   * @param rejection why it is not, otherwise
   * @param message detail for the rejection
   */
  public record Result(
      Optional<Admitted> admitted, TransactionInvalidReason rejection, String message) {
    static Result reject(final TransactionInvalidReason reason, final String message) {
      return new Result(Optional.empty(), reason, message);
    }

    /**
     * Whether the transaction is admissible.
     *
     * @return true if admitted
     */
    public boolean isAdmitted() {
      return admitted.isPresent();
    }
  }

  /**
   * Creates the admission check.
   *
   * @param protocolSchedule the protocol schedule
   * @param worldStateArchive the world state archive to simulate against
   */
  public FrameTransactionAdmission(
      final ProtocolSchedule protocolSchedule, final WorldStateArchive worldStateArchive) {
    this.protocolSchedule = protocolSchedule;
    this.worldStateArchive = worldStateArchive;
  }

  /**
   * Checks a frame transaction against the head.
   *
   * @param transaction the frame transaction
   * @param head the chain head
   * @return the decision
   */
  public Result admit(final Transaction transaction, final BlockHeader head) {
    if (transaction.getVersionedHashes().map(h -> !h.isEmpty()).orElse(false)) {
      return Result.reject(
          TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION,
          "blob-carrying frame transactions are not yet supported by the transaction pool");
    }

    final ValidationPrefix prefix;
    try {
      prefix = ValidationPrefix.classify(transaction);
    } catch (final ValidationPrefix.UnrecognizedPrefixException e) {
      return Result.reject(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION, e.getMessage());
    }
    if (prefix.expiryDeadline().isPresent()
        && Long.compareUnsigned(prefix.expiryDeadline().getAsLong(), head.getTimestamp()) < 0) {
      return Result.reject(
          TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION, "expiry deadline has passed");
    }

    final ProtocolSpec spec = protocolSchedule.getForNextBlockHeader(head, head.getTimestamp() + 1);
    final Optional<FrameTransactionProcessor> processor =
        spec.getTransactionProcessor().getFrameTransactionProcessor();
    if (processor.isEmpty()) {
      return Result.reject(
          TransactionInvalidReason.INVALID_TRANSACTION_FORMAT,
          "frame transactions are not active at the next block");
    }

    try (final MutableWorldState worldState =
        worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(head)).orElseThrow()) {
      final Address sender = transaction.getSender();
      final WorldUpdater simulation = worldState.updater();
      final ValidationTraceRules rules =
          new ValidationTraceRules(sender, prefix, spec.getPrecompileContractRegistry());
      final ValidationPrefixResult result =
          processor
              .get()
              .simulateValidationPrefix(
                  simulation,
                  head,
                  transaction,
                  head.getCoinbase(),
                  rules,
                  (f, n) -> Hash.ZERO,
                  Wei.ZERO);

      if (rules.violation().isPresent()) {
        return Result.reject(
            TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION, rules.violation().get());
      }
      if (!result.validity().isValid()) {
        return Result.reject(
            result.validity().getInvalidReason(), result.validity().getErrorMessage());
      }
      if (result.payerFrameIndex() != prefix.payFrame()) {
        return Result.reject(
            TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION,
            "payment approved by frame "
                + result.payerFrameIndex()
                + ", expected frame "
                + prefix.payFrame());
      }
      if (prefix.hasDeploy()) {
        final Account deployed = simulation.get(sender);
        if (deployed == null || deployed.getCode().isEmpty()) {
          return Result.reject(
              TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION,
              "deploy frame installed no code at the sender");
        }
      }

      final Address payer = result.payer().orElseThrow();
      final Account payerAccount = worldState.get(payer);
      final Wei payerBalance = payerAccount == null ? Wei.ZERO : payerAccount.getBalance();
      final boolean codedPaymaster =
          !payer.equals(sender) && payerAccount != null && !payerAccount.getCode().isEmpty();
      return new Result(
          Optional.of(
              new Admitted(prefix, payer, maxCost(transaction), payerBalance, codedPaymaster)),
          null,
          null);
    } catch (final Exception e) {
      LOG.debug("Frame transaction {} could not be simulated", transaction.getHash(), e);
      return Result.reject(
          TransactionInvalidReason.CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE, e.getMessage());
    }
  }

  /** The most a frame transaction's payer can be charged: {@code TXPARAM(0x06)} without blobs. */
  static Wei maxCost(final Transaction transaction) {
    final BigInteger cost =
        transaction
            .getMaxGasPrice()
            .getAsBigInteger()
            .multiply(BigInteger.valueOf(transaction.getGasLimit()));
    return cost.bitLength() > 256 ? Wei.MAX_WEI : Wei.of(cost);
  }
}
