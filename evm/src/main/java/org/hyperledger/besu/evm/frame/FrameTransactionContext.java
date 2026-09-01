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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.collections.undo.UndoScalar;
import org.hyperledger.besu.collections.undo.UndoTable;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.List;

import com.google.common.collect.TreeBasedTable;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Transaction-scoped context of an EIP-8141 frame transaction, shared by all EVM call frames of
 * every frame of the transaction via {@link TxValues}.
 *
 * <p>The immutable inputs describe the transaction; the mutable approval context (payer, sender
 * approval), the per-frame state-gas receipts and the outstanding SSTORE charge owners are
 * journaled with the same global undo clock as the rest of {@link TxValues} so they roll back at
 * exactly the same boundaries as the state changes that produced them.
 */
public class FrameTransactionContext {

  /** DEFAULT frame mode: execute with ENTRY_POINT as caller. */
  public static final int MODE_DEFAULT = 0;

  /** VERIFY frame mode: execute statically as transaction validation. */
  public static final int MODE_VERIFY = 1;

  /** SENDER frame mode: execute with the transaction sender as caller. */
  public static final int MODE_SENDER = 2;

  /** Approval scope bit for payment. */
  public static final int APPROVE_PAYMENT = 0x1;

  /** Approval scope bit for sender execution. */
  public static final int APPROVE_EXECUTION = 0x2;

  /** Mask of the valid approval scope bits. */
  public static final int APPROVE_SCOPE_MASK = 0x3;

  /** Flag bit marking a frame as atomically batched with its successor. */
  public static final int ATOMIC_BATCH_FLAG = 0x4;

  /** ARBITRARY signature scheme. */
  public static final int SCHEME_ARBITRARY = 0x0;

  /** SECP256K1 signature scheme. */
  public static final int SCHEME_SECP256K1 = 0x1;

  /** P256 signature scheme. */
  public static final int SCHEME_P256 = 0x2;

  /** Frame receipt status: failure. */
  public static final int STATUS_FAILURE = 0;

  /** Frame receipt status: success. */
  public static final int STATUS_SUCCESS = 1;

  /** Frame receipt status: skipped because an atomic batch failed. */
  public static final int STATUS_SKIPPED = 2;

  /** The EIP-8141 ENTRY_POINT caller address of DEFAULT and VERIFY frames. */
  public static final Address ENTRY_POINT = Address.fromHexString("0xaa");

  /**
   * Immutable description of one frame, with the target already resolved against the sender.
   *
   * @param mode the frame mode
   * @param flags the frame flags
   * @param resolvedTarget the target address, already defaulted to the sender when absent
   * @param executionGasLimit the frame execution gas budget
   * @param stateGasLimit the frame state gas budget
   * @param value the wei transferred by a SENDER frame
   * @param data the frame calldata
   */
  public record FrameInfo(
      int mode,
      int flags,
      Address resolvedTarget,
      long executionGasLimit,
      long stateGasLimit,
      Wei value,
      Bytes data) {}

  /**
   * Immutable description of one signature entry.
   *
   * @param scheme the signature scheme
   * @param resolvedSigner the signer resolved against the sender, or null for ARBITRARY entries
   * @param msg the explicit 32-byte digest, or null when the entry signs the canonical hash
   * @param arbitraryBytes the raw signature bytes for ARBITRARY entries, null otherwise
   */
  public record SignatureInfo(
      int scheme, Address resolvedSigner, Bytes32 msg, Bytes arbitraryBytes) {}

  private final List<FrameInfo> frames;
  private final List<SignatureInfo> signatures;
  private final Address sender;
  private final Bytes32 signatureHash;
  private final long nonce;
  private final Wei maxPriorityFeePerGas;
  private final Wei maxFeePerGas;
  private final Wei maxFeePerBlobGas;
  private final Wei maxCost;

  private int currentFrameIndex = 0;

  // Frame receipt fields that are final once the frame completes.
  private final int[] frameStatuses;
  private final long[] frameExecutionGasUsed;

  // Frame receipt state gas stays live until the transaction ends: refills by later frames lower
  // it and rollbacks restore it, so it is journaled.
  private final List<UndoScalar<Long>> frameStateGasUsed;

  private final UndoScalar<Address> payer;
  private final UndoScalar<Boolean> senderApproved;

  // The frame that owns the outstanding SSTORE state charge per (address, slot). EIP-8037 refill
  // reattribution needs the owner so a cross-frame refill reduces the right receipt.
  private final UndoTable<Address, Bytes32, Integer> slotChargeOwners;

  /**
   * Creates the context.
   *
   * @param frames the transaction frames with resolved targets
   * @param signatures the transaction signature entries
   * @param sender the transaction sender
   * @param signatureHash the canonical transaction signature hash
   * @param nonce the transaction nonce
   * @param maxPriorityFeePerGas the max priority fee per gas
   * @param maxFeePerGas the max fee per gas
   * @param maxFeePerBlobGas the max fee per blob gas
   * @param maxCost the maximum wei cost collected from the payer at approval
   */
  public FrameTransactionContext(
      final List<FrameInfo> frames,
      final List<SignatureInfo> signatures,
      final Address sender,
      final Bytes32 signatureHash,
      final long nonce,
      final Wei maxPriorityFeePerGas,
      final Wei maxFeePerGas,
      final Wei maxFeePerBlobGas,
      final Wei maxCost) {
    this.frames = frames;
    this.signatures = signatures;
    this.sender = sender;
    this.signatureHash = signatureHash;
    this.nonce = nonce;
    this.maxPriorityFeePerGas = maxPriorityFeePerGas;
    this.maxFeePerGas = maxFeePerGas;
    this.maxFeePerBlobGas = maxFeePerBlobGas;
    this.maxCost = maxCost;
    this.frameStatuses = new int[frames.size()];
    this.frameExecutionGasUsed = new long[frames.size()];
    this.frameStateGasUsed = new java.util.ArrayList<>(frames.size());
    for (int i = 0; i < frames.size(); i++) {
      this.frameStateGasUsed.add(new UndoScalar<>(0L));
    }
    this.payer = new UndoScalar<>(null);
    this.senderApproved = new UndoScalar<>(Boolean.FALSE);
    this.slotChargeOwners = UndoTable.of(TreeBasedTable.create());
  }

  /**
   * Rolls the journaled context back to the given global undo mark.
   *
   * @param mark the mark to roll back to
   */
  void undoChanges(final long mark) {
    payer.undo(mark);
    senderApproved.undo(mark);
    slotChargeOwners.undo(mark);
    for (final UndoScalar<Long> stateGas : frameStateGasUsed) {
      stateGas.undo(mark);
    }
  }

  /**
   * The transaction frames.
   *
   * @return the frames
   */
  public List<FrameInfo> frames() {
    return frames;
  }

  /**
   * The transaction signature entries.
   *
   * @return the signature entries
   */
  public List<SignatureInfo> signatures() {
    return signatures;
  }

  /**
   * The transaction sender.
   *
   * @return the sender
   */
  public Address sender() {
    return sender;
  }

  /**
   * The canonical signature hash.
   *
   * @return the signature hash
   */
  public Bytes32 signatureHash() {
    return signatureHash;
  }

  /**
   * The transaction nonce.
   *
   * @return the nonce
   */
  public long nonce() {
    return nonce;
  }

  /**
   * The max priority fee per gas.
   *
   * @return the max priority fee per gas
   */
  public Wei maxPriorityFeePerGas() {
    return maxPriorityFeePerGas;
  }

  /**
   * The max fee per gas.
   *
   * @return the max fee per gas
   */
  public Wei maxFeePerGas() {
    return maxFeePerGas;
  }

  /**
   * The max fee per blob gas.
   *
   * @return the max fee per blob gas
   */
  public Wei maxFeePerBlobGas() {
    return maxFeePerBlobGas;
  }

  /**
   * The maximum cost collected from the payer at approval.
   *
   * @return the max cost in wei
   */
  public Wei maxCost() {
    return maxCost;
  }

  /**
   * The index of the currently executing frame.
   *
   * @return the current frame index
   */
  public int currentFrameIndex() {
    return currentFrameIndex;
  }

  /**
   * Moves execution to the given frame.
   *
   * @param frameIndex the frame about to execute
   */
  public void setCurrentFrameIndex(final int frameIndex) {
    this.currentFrameIndex = frameIndex;
  }

  /**
   * The currently executing frame.
   *
   * @return the current frame info
   */
  public FrameInfo currentFrame() {
    return frames.get(currentFrameIndex);
  }

  /**
   * The recorded status of a completed frame.
   *
   * @param frameIndex the frame index
   * @return the status (0 failure, 1 success, 2 skipped)
   */
  public int frameStatus(final int frameIndex) {
    return frameStatuses[frameIndex];
  }

  /**
   * The recorded execution gas of a completed frame. Immutable once recorded.
   *
   * @param frameIndex the frame index
   * @return the execution gas used
   */
  public long frameExecutionGasUsed(final int frameIndex) {
    return frameExecutionGasUsed[frameIndex];
  }

  /**
   * The live attributed state gas of a frame's receipt.
   *
   * @param frameIndex the frame index
   * @return the state gas currently attributed to the frame
   */
  public long frameStateGasUsed(final int frameIndex) {
    return frameStateGasUsed.get(frameIndex).get();
  }

  /**
   * Records a completed frame's receipt.
   *
   * @param frameIndex the frame index
   * @param status the frame status
   * @param executionGasUsed the execution gas used
   * @param stateGasUsed the state gas attributed at frame exit
   */
  public void recordFrameCompletion(
      final int frameIndex,
      final int status,
      final long executionGasUsed,
      final long stateGasUsed) {
    frameStatuses[frameIndex] = status;
    frameExecutionGasUsed[frameIndex] = executionGasUsed;
    frameStateGasUsed.get(frameIndex).set(stateGasUsed);
  }

  /**
   * Marks a frame as skipped by a failed atomic batch.
   *
   * @param frameIndex the frame index
   */
  public void recordFrameSkipped(final int frameIndex) {
    frameStatuses[frameIndex] = STATUS_SKIPPED;
    frameExecutionGasUsed[frameIndex] = 0L;
    frameStateGasUsed.get(frameIndex).set(0L);
  }

  /**
   * The payer, when approved.
   *
   * @return the payer or null
   */
  public Address payer() {
    return payer.get();
  }

  /**
   * Sets the payer.
   *
   * @param payerAddress the account paying the transaction fees
   */
  public void setPayer(final Address payerAddress) {
    payer.set(payerAddress);
  }

  /**
   * Whether sender execution has been approved.
   *
   * @return true once APPROVE_EXECUTION succeeded
   */
  public boolean senderApproved() {
    return senderApproved.get();
  }

  /** Marks sender execution as approved. */
  public void approveSender() {
    senderApproved.set(Boolean.TRUE);
  }

  /**
   * Records the current frame as owner of the outstanding SSTORE state charge for a slot.
   *
   * @param address the account address
   * @param slotKey the storage slot
   */
  public void recordSlotCharge(final Address address, final Bytes32 slotKey) {
    slotChargeOwners.put(address, slotKey, currentFrameIndex);
  }

  /**
   * Removes and returns the owner of the outstanding SSTORE charge for a slot.
   *
   * @param address the account address
   * @param slotKey the storage slot
   * @return the owning frame index, or null when no charge is outstanding
   */
  public Integer removeSlotCharge(final Address address, final Bytes32 slotKey) {
    return slotChargeOwners.remove(address, slotKey);
  }

  /**
   * Reattributes a cross-frame state-gas refill: reduces the owning frame's receipt without
   * crediting any spendable pool.
   *
   * @param ownerFrameIndex the frame that paid the outstanding charge
   * @param amount the refill amount
   */
  public void reduceFrameStateGasUsed(final int ownerFrameIndex, final long amount) {
    final UndoScalar<Long> receipt = frameStateGasUsed.get(ownerFrameIndex);
    receipt.set(receipt.get() - amount);
  }
}
