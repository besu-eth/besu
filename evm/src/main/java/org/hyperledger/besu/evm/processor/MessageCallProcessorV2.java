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
package org.hyperledger.besu.evm.processor;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;
import java.util.Set;

/**
 * The message call processor of an EVM v2. A call without value to a contract only touches its
 * recipient, so it skips the sender lookup and the value checks of a transfer. Every other call
 * starts as in {@link MessageCallProcessor}.
 */
public class MessageCallProcessorV2 extends MessageCallProcessor {

  /**
   * Instantiates a new message call processor for EVM v2.
   *
   * @param evm the EVM, which runs on EVM v2
   * @param precompiles the precompiles
   * @param forceCommitAddresses the addresses committed even when the frame fails
   * @param transferLogEmitter the emitter of value transfer logs
   */
  public MessageCallProcessorV2(
      final EVM evm,
      final PrecompileContractRegistry precompiles,
      final Set<Address> forceCommitAddresses,
      final TransferLogEmitter transferLogEmitter) {
    super(evm, precompiles, forceCommitAddresses, transferLogEmitter);
  }

  /**
   * Creates the message call processor for an EVM: this one for EVM v2, {@link
   * MessageCallProcessor} for EVM v1.
   *
   * @param evm the EVM
   * @param precompiles the precompiles
   * @param forceCommitAddresses the addresses committed even when the frame fails
   * @param transferLogEmitter the emitter of value transfer logs
   * @return the message call processor
   */
  public static MessageCallProcessor forEvm(
      final EVM evm,
      final PrecompileContractRegistry precompiles,
      final Set<Address> forceCommitAddresses,
      final TransferLogEmitter transferLogEmitter) {
    return evm.getEvmConfiguration().enableEvmV2()
        ? new MessageCallProcessorV2(evm, precompiles, forceCommitAddresses, transferLogEmitter)
        : new MessageCallProcessor(evm, precompiles, forceCommitAddresses, transferLogEmitter);
  }

  @Override
  public void start(final MessageFrame frame, final OperationTracer operationTracer) {
    if (!frame.getValue().isZero() || precompiles.get(frame.getContractAddress()) != null) {
      super.start(frame, operationTracer);
      return;
    }
    try {
      // the recipient is created even when the call sends no value
      frame.getWorldUpdater().getOrCreate(frame.getRecipientAddress());
      frame.setState(MessageFrame.State.CODE_EXECUTING);
    } catch (final ModificationNotAllowedException ex) {
      frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE));
      frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
    }
  }
}
