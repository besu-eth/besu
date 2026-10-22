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
package org.hyperledger.besu.evm.v2.operation;

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedIntAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setAddress;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setBytes32;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setWei;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.KeyedNonces;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.TxParamOperation;

import java.util.List;

/** The EIP-8141 TXPARAM operation (0xb0): transaction-scoped introspection. */
public class TxParamOperationV2 extends AbstractOperationV2 {

  private static final long GAS_COST = 2L;
  private static final OperationResult SUCCESS = new OperationResult(GAS_COST, null);
  private static final OperationResult INVALID =
      new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxParamOperationV2(final GasCalculator gasCalculator) {
    super(TxParamOperation.OPCODE, "TXPARAM", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return INVALID;
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!frame.stackHasItemsV2(1)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int slot = frame.stackTopV2() - 1;

    switch (readClampedIntAt(stack, slot + 1, 0)) {
      case 0x00 -> setLong(0x06, stack, slot);
      case 0x01 -> setLong(context.nonce(), stack, slot);
      case 0x02 -> setAddress(context.sender(), stack, slot);
      case 0x03 -> setWei(context.maxPriorityFeePerGas(), stack, slot);
      case 0x04 -> setWei(context.maxFeePerGas(), stack, slot);
      case 0x05 -> setWei(context.maxFeePerBlobGas(), stack, slot);
      case 0x06 -> setWei(context.maxCost(), stack, slot);
      case 0x07 -> setLong(frame.getVersionedHashes().map(List::size).orElse(0), stack, slot);
      case 0x08 -> setBytes32(context.signatureHash(), stack, slot);
      case 0x09 -> setLong(context.frames().size(), stack, slot);
      case 0x0A -> setLong(context.currentFrameIndex(), stack, slot);
      case 0x0B -> setLong(context.signatures().size(), stack, slot);
      case 0x0C -> setLong(frame.getStateGasReservoir(), stack, slot);
      case 0x0D -> setLong(context.legacyNonce(), stack, slot);
      case 0x0E -> setLong(context.nonceKeys().size(), stack, slot);
      case 0x0F -> setBytes32(KeyedNonces.nonceKeysHash(context.nonceKeys()), stack, slot);
      case 0x10 -> setBytes32(context.nonceKeys().get(0), stack, slot);
      default -> {
        return haltPopping(frame, 1, INVALID);
      }
    }
    return SUCCESS;
  }
}
