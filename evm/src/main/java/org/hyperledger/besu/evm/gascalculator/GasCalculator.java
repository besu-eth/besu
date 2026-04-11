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
package org.hyperledger.besu.evm.gascalculator;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.BalanceOperation;
import org.hyperledger.besu.evm.operation.BlockHashOperation;
import org.hyperledger.besu.evm.operation.ExpOperation;
import org.hyperledger.besu.evm.operation.ExtCodeCopyOperation;
import org.hyperledger.besu.evm.operation.ExtCodeHashOperation;
import org.hyperledger.besu.evm.operation.ExtCodeSizeOperation;
import org.hyperledger.besu.evm.operation.JumpDestOperation;
import org.hyperledger.besu.evm.operation.Keccak256Operation;
import org.hyperledger.besu.evm.operation.LogOperation;
import org.hyperledger.besu.evm.operation.MLoadOperation;
import org.hyperledger.besu.evm.operation.MStore8Operation;
import org.hyperledger.besu.evm.operation.MStoreOperation;
import org.hyperledger.besu.evm.operation.SLoadOperation;
import org.hyperledger.besu.evm.operation.SelfDestructOperation;
import org.hyperledger.besu.evm.precompile.ECRECPrecompiledContract;
import org.hyperledger.besu.evm.precompile.IDPrecompiledContract;
import org.hyperledger.besu.evm.precompile.RIPEMD160PrecompiledContract;
import org.hyperledger.besu.evm.precompile.SHA256PrecompiledContract;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;

import java.util.List;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Provides various gas cost lookups and calculations used during block processing.
 *
 * <p>The {@code GasCalculator} is meant to encapsulate all Gas-related calculations except for the
 * following "safe" operations:
 *
 * <ul>
 *   <li><b>Operation Gas Deductions:</b> Deducting the operation's gas cost from the VM's current
 *       message frame because the
 * </ul>
 */
public interface GasCalculator {

  // Precompiled Contract Gas Calculations

  /**
   * Returns the gas cost to execute the {@link IDPrecompiledContract}.
   *
   * @param input The input to the ID precompiled contract
   * @return the gas cost to execute the ID precompiled contract
   */
  long idPrecompiledContractGasCost(Bytes input);

  /**
   * Returns the gas cost to execute the {@link ECRECPrecompiledContract}.
   *
   * @return the gas cost to execute the ECREC precompiled contract
   */
  long getEcrecPrecompiledContractGasCost();

  /**
   * Returns the gas cost to execute the {@link ECRECPrecompiledContract}.
   *
   * @return the gas cost to execute the P256Verify precompiled contract
   */
  long getP256VerifyPrecompiledContractGasCost();

  /**
   * Returns the gas cost to execute the {@link SHA256PrecompiledContract}.
   *
   * @param input The input to the SHA256 precompiled contract
   * @return the gas cost to execute the SHA256 precompiled contract
   */
  long sha256PrecompiledContractGasCost(Bytes input);

  /**
   * Returns the gas cost to execute the {@link RIPEMD160PrecompiledContract}.
   *
   * @param input The input to the RIPEMD160 precompiled contract
   * @return the gas cost to execute the RIPEMD160 precompiled contract
   */
  long ripemd160PrecompiledContractGasCost(Bytes input);

  // Gas Tier Lookups

  /**
   * Returns the gas cost for the zero gas tier.
   *
   * @return the gas cost for the zero gas tier
   */
  long getZeroTierGasCost();

  /**
   * Returns the gas cost for the very low gas tier.
   *
   * @return the gas cost for the very low gas tier
   */
  long getVeryLowTierGasCost();

  /**
   * Returns the gas cost for the low gas tier.
   *
   * @return the gas cost for the low gas tier
   */
  long getLowTierGasCost();

  /**
   * Returns the gas cost for the base gas tier.
   *
   * @return the gas cost for the base gas tier
   */
  long getBaseTierGasCost();

  /**
   * Returns the gas cost for the mid gas tier.
   *
   * @return the gas cost for the mid gas tier
   */
  long getMidTierGasCost();

  /**
   * Returns the gas cost for the high gas tier.
   *
   * @return the gas cost for the high gas tier
   */
  long getHighTierGasCost();

  // Call/Create Operation Calculations

  /**
   * Returns the base gas cost to execute a call operation.
   *
   * @return the base gas cost to execute a call operation
   */
  long callOperationBaseGasCost();

  /**
   * Returns the gas cost to transfer funds in a call operation.
   *
   * @return the gas cost to transfer funds in a call operation
   */
  long callValueTransferGasCost();

  /**
   * Returns the gas cost to create a new account.
   *
   * @return the gas cost to create a new account
   */
  long newAccountGasCost();

  /**
   * Returns the total call cost given a pre-computed static call cost.
   *
   * @param frame The current frame
   * @param staticCallCost The static call cost
   * @param stipend The gas stipend being provided by the CALL caller
   * @param inputDataOffset The offset in memory to retrieve the CALL input data
   * @param inputDataLength The CALL input data length
   * @param outputDataOffset The offset in memory to place the CALL output data
   * @param outputDataLength The CALL output data length
   * @param transferValue The wei being transferred
   * @param recipientAddress The CALL recipient (may be null if self destructed or new) address
   * @param accountIsWarm The address of the contract is "warm" as per EIP-2929
   * @return The gas cost for the CALL operation
   */
  long callOperationGasCost(
      final MessageFrame frame,
      final long staticCallCost,
      final long stipend,
      final long inputDataOffset,
      final long inputDataLength,
      final long outputDataOffset,
      final long outputDataLength,
      final Wei transferValue,
      final Address recipientAddress,
      final boolean accountIsWarm);

  /**
   * Returns the static gas cost to execute a call operation.
   *
   * <p>This method <strong>must not</strong> access or mutate world state and it must not mutate
   * {@code frame.getEip7928AccessList()}.
   *
   * @param frame The current frame
   * @param stipend The gas stipend being provided by the CALL caller
   * @param inputDataOffset The offset in memory to retrieve the CALL input data
   * @param inputDataLength The CALL input data length
   * @param outputDataOffset The offset in memory to place the CALL output data
   * @param outputDataLength The CALL output data length
   * @param transferValue The wei being transferred
   * @param recipientAddress The CALL recipient (may be null if self destructed or new) address
   * @param accountIsWarm The address of the contract is "warm" as per EIP-2929
   * @return The static gas cost for the CALL operation
   */
  long callOperationStaticGasCost(
      final MessageFrame frame,
      final long stipend,
      final long inputDataOffset,
      final long inputDataLength,
      final long outputDataOffset,
      final long outputDataLength,
      final Wei transferValue,
      final Address recipientAddress,
      final boolean accountIsWarm);

  /**
   * Gets additional call stipend.
   *
   * @return the additional call stipend
   */
  long getAdditionalCallStipend();

  /**
   * Returns the amount of gas parent will provide its child CALL.
   *
   * @param frame The current frame
   * @param stipend The gas stipend being provided by the CALL caller
   * @param transfersValue Whether call transfers any wei
   * @return the amount of gas parent will provide its child CALL
   */
  long gasAvailableForChildCall(MessageFrame frame, long stipend, boolean transfersValue);

  /**
   * For EXT*CALL, the minimum amount of gas the parent must retain. First described in EIP-7069
   *
   * @return MIN_RETAINED_GAS
   */
  long getMinRetainedGas();

  /**
   * For EXT*CALL, the minimum amount of gas that a child must receive. First described in EIP-7069
   *
   * @return MIN_CALLEE_GAS
   */
  long getMinCalleeGas();

  /**
   * Returns the base create cost, or TX_CREATE_COST as defined in the execution specs
   *
   * @return the TX_CREATE value for this gas schedule
   */
  long txCreateCost();

  /**
   * For Creates that need to hash the initcode, this is the gas cost for such hashing
   *
   * @param initCodeLength length of the init code, in bytes
   * @return gas cost to charge for hashing
   */
  long createKeccakCost(int initCodeLength);

  /**
   * The cost of a create operation's initcode charge. This is just the initcode cost, separate from
   * the operation base cost and initcode hashing cost.
   *
   * @param initCodeLength Number of bytes in the initcode
   * @return the gas cost for the create initcode
   */
  long initcodeCost(final int initCodeLength);

  /**
   * Returns the amount of gas parent will provide its child CREATE.
   *
   * @param stipend The gas stipend being provided by the CREATE caller
   * @return the amount of gas parent will provide its child CREATE
   */
  long gasAvailableForChildCreate(long stipend);

  // Re-used Operation Calculations

  /**
   * Returns the amount of gas consumed by the data copy operation.
   *
   * @param frame The current frame
   * @param offset The offset in memory to copy the data to
   * @param length The length of the data being copied into memory
   * @return the amount of gas consumed by the data copy operation
   */
  long dataCopyOperationGasCost(MessageFrame frame, long offset, long length);

  /**
   * Returns the cost of expanding memory for the specified access.
   *
   * @param frame The current frame
   * @param offset The offset in memory where the access occurs
   * @param length the length of the memory access
   * @return The gas required to expand memory for the specified access
   */
  long memoryExpansionGasCost(MessageFrame frame, long offset, long length);

  // Specific Non-call Operation Calculations

  /**
   * Returns the cost for executing a {@link BalanceOperation}.
   *
   * @return the cost for executing the balance operation
   */
  long getBalanceOperationGasCost();

  /**
   * Returns the cost for executing a {@link BlockHashOperation}.
   *
   * @return the cost for executing the block hash operation
   */
  long getBlockHashOperationGasCost();

  /**
   * Returns the cost for executing a {@link ExpOperation}.
   *
   * @param numBytes The number of bytes for the exponent parameter
   * @return the cost for executing the exp operation
   */
  long expOperationGasCost(int numBytes);

  /**
   * Returns the cost for executing a {@link ExtCodeCopyOperation}.
   *
   * @param frame The current frame
   * @param offset The offset in memory to external code copy the data to
   * @param length The length of the code being copied into memory
   * @return the cost for executing the external code size operation
   */
  long extCodeCopyOperationGasCost(MessageFrame frame, long offset, long length);

  /**
   * Returns the cost for executing a {@link ExtCodeHashOperation}.
   *
   * @return the cost for executing the external code hash operation
   */
  long extCodeHashOperationGasCost();

  /**
   * Returns the cost for executing a {@link ExtCodeSizeOperation}.
   *
   * @return the cost for executing the external code size operation
   */
  long getExtCodeSizeOperationGasCost();

  /**
   * Returns the cost for executing a {@link JumpDestOperation}.
   *
   * @return the cost for executing the jump destination operation
   */
  long getJumpDestOperationGasCost();

  /**
   * Returns the cost for executing a {@link LogOperation}.
   *
   * @param frame The current frame
   * @param dataOffset The offset in memory where the log data exists
   * @param dataLength The length of the log data to read from memory
   * @param numTopics The number of topics in the log
   * @return the cost for executing the external code size operation
   */
  long logOperationGasCost(MessageFrame frame, long dataOffset, long dataLength, int numTopics);

  /**
   * Returns the cost for executing a {@link MLoadOperation}.
   *
   * @param frame The current frame
   * @param offset The offset in memory where the access takes place
   * @return the cost for executing the memory load operation
   */
  long mLoadOperationGasCost(MessageFrame frame, long offset);

  /**
   * Returns the cost for executing a {@link MStoreOperation}.
   *
   * @param frame The current frame
   * @param offset The offset in memory where the access takes place
   * @return the cost for executing the memory store operation
   */
  long mStoreOperationGasCost(MessageFrame frame, long offset);

  /**
   * Returns the cost for executing a {@link MStore8Operation}.
   *
   * @param frame The current frame
   * @param offset The offset in memory where the access takes place
   * @return the cost for executing the memory byte store operation
   */
  long mStore8OperationGasCost(MessageFrame frame, long offset);

  /**
   * Returns the cost for executing a {@link SelfDestructOperation}.
   *
   * @param recipient The recipient of the self destructed inheritance (may be null)
   * @param inheritance The amount the recipient will receive
   * @return the cost for executing the self destruct operation
   */
  default long selfDestructOperationGasCost(final Account recipient, final Wei inheritance) {
    return selfDestructOperationStaticGasCost();
  }

  /**
   * Returns the static cost for executing a {@link SelfDestructOperation}.
   *
   * @return the base cost for executing a {@link SelfDestructOperation}
   */
  default long selfDestructOperationStaticGasCost() {
    return 0L;
  }

  /**
   * Returns the cost for executing a {@link Keccak256Operation}.
   *
   * @param frame The current frame
   * @param offset The offset in memory where the data to be hashed exists
   * @param length The hashed data length
   * @return the cost for executing the memory byte store operation
   */
  long keccak256OperationGasCost(MessageFrame frame, long offset, long length);

  /**
   * Returns the cost for executing a {@link SLoadOperation}.
   *
   * @return the cost for executing the storage load operation
   */
  long getSloadOperationGasCost();

  /**
   * Returns the cost for an SSTORE operation.
   *
   * @param newValue the new value to be stored
   * @param currentValue the supplier of the current value
   * @param originalValue the supplier of the original value
   * @return the gas cost for the SSTORE operation
   */
  long calculateStorageCost(
      UInt256 newValue, Supplier<UInt256> currentValue, Supplier<UInt256> originalValue);

  /**
   * Returns the refund amount for an SSTORE operation.
   *
   * @param newValue the new value to be stored
   * @param currentValue the supplier of the current value
   * @param originalValue the supplier of the original value
   * @return the gas refund for the SSTORE operation
   */
  long calculateStorageRefundAmount(
      UInt256 newValue, Supplier<UInt256> currentValue, Supplier<UInt256> originalValue);

  /**
   * Returns the refund amount for deleting an account in a {@link SelfDestructOperation}.
   *
   * @return the refund amount for deleting an account in a self destruct operation
   */
  long getSelfDestructRefundAmount();

  /**
   * Returns the cost of a SLOAD to a storage slot not previously loaded in the TX context.
   *
   * @return the cost of a SLOAD to a storage slot not previously loaded in the TX context.
   */
  default long getColdSloadCost() {
    return 0L;
  }

  /**
   * Returns the cost to access an account not previously accessed in the TX context.
   *
   * @return the cost to access an account not previously accessed in the TX context.
   */
  default long getColdAccountAccessCost() {
    return 0L;
  }

  /**
   * Returns the cost of a SLOAD to a storage slot that has previously been loaded in the TX
   * context.
   *
   * @return the cost of a SLOAD to a storage slot that has previously been loaded in the TX
   *     context.
   */
  default long getWarmStorageReadCost() {
    return 0L;
  }

  /**
   * For the purposes of this gas calculator, is this address a precompile?
   *
   * @param address the address to test for being a precompile
   * @return true if it is a precompile.
   */
  default boolean isPrecompile(final Address address) {
    return false;
  }

  /**
   * Mod exp gas cost.
   *
   * @param input the input
   * @return the long
   */
  default long modExpGasCost(final Bytes input) {
    return 0L;
  }

  /**
   * Returns the cost for a {@link AbstractMessageProcessor} to deposit the code in storage
   *
   * @param codeSize The size of the code in bytes
   * @return the code deposit cost
   */
  long codeDepositGasCost(int codeSize);

  /**
   * Returns the intrinsic gas cost of a transaction payload, i.e. the cost deriving from its
   * encoded binary representation when stored on-chain.
   *
   * @param transaction The encoded transaction
   * @param baselineGas The gas used by access lists and code delegation authorizations
   * @return the transaction's intrinsic gas cost
   */
  long transactionIntrinsicGasCost(Transaction transaction, long baselineGas);

  /**
   * Returns the floor gas cost of a transaction payload, i.e. the minimum gas cost that a
   * transaction will be charged based on its calldata. Introduced in EIP-7623 in Prague.
   *
   * @param transactionPayload The encoded transaction, as bytes
   * @param payloadZeroBytes The number of zero bytes in the payload
   * @return the transaction's floor gas cost
   */
  long transactionFloorCost(final Bytes transactionPayload, final long payloadZeroBytes);

  /**
   * Returns the gas cost of the explicitly declared access list.
   *
   * @param accessListEntries The access list entries
   * @return the access list's gas cost
   */
  default long accessListGasCost(final List<AccessListEntry> accessListEntries) {
    return accessListGasCost(
        accessListEntries.size(),
        accessListEntries.stream().mapToInt(e -> e.storageKeys().size()).sum());
  }

  /**
   * Returns the gas cost of the explicitly declared access list.
   *
   * @param addresses The count of addresses accessed
   * @param storageSlots The count of storage slots accessed
   * @return the access list's gas cost
   */
  default long accessListGasCost(final int addresses, final int storageSlots) {
    return 0L;
  }

  /**
   * A measure of the maximum amount of refunded gas a transaction will be credited with.
   *
   * @return the quotient of the equation `txGasCost / refundQuotient`.
   */
  default long getMaxRefundQuotient() {
    return 2;
  }

  /**
   * Minimum gas cost of a transaction.
   *
   * @return the minimum gas cost
   */
  long getMinimumTransactionCost();

  /**
   * Returns the cost of a loading from Transient Storage
   *
   * @return the cost of a TLOAD from a storage slot
   */
  default long getTransientLoadOperationGasCost() {
    return 0L;
  }

  /**
   * Returns the cost of a storing to Transient Storage
   *
   * @return the cost of a TSTORE to a storage slot
   */
  default long getTransientStoreOperationGasCost() {
    return 0L;
  }

  /**
   * Returns the gas cost per item for the block access list size constraint (EIP-7928).
   *
   * @return the cost per BAL item (address or storage key) for the size limit; 0 if BAL size
   *     constraint is not applicable for this fork
   */
  default long getBlockAccessListItemCost() {
    return 0L;
  }

  /**
   * Returns the blob gas cost per blob. This is the gas cost for each blob of data that is added to
   * the block.
   *
   * @return the blob gas cost per blob
   */
  default long getBlobGasPerBlob() {
    return 0L;
  }

  /**
   * Return the gas cost given the number of blobs
   *
   * @param blobCount the number of blobs
   * @return the total gas cost
   */
  default long blobGasCost(final long blobCount) {
    return 0L;
  }

  /**
   * Returns the upfront gas cost for EIP 7702 authorization processing.
   *
   * @param delegateCodeListLength The length of the code delegation list
   * @return the gas cost
   */
  default long delegateCodeGasCost(final int delegateCodeListLength) {
    return 0L;
  }

  /**
   * Calculates the refund for processing the 7702 code delegation list if a delegator account
   * already exists in the trie.
   *
   * @param alreadyExistingAccountSize The number of accounts already in the trie
   * @return the gas refund
   */
  default long calculateDelegateCodeGasRefund(final long alreadyExistingAccountSize) {
    return 0L;
  }

  /**
   * Calculate the gas refund for a transaction.
   *
   * @param transaction the transaction
   * @param initialFrame the initial frame
   * @param codeDelegationRefund the code delegation refund
   * @return the gas refund
   */
  long calculateGasRefund(
      Transaction transaction, MessageFrame initialFrame, long codeDelegationRefund);

  /**
   * Compute the gas cost for delegated code resolution.
   *
   * @param frame the message frame
   * @param targetAccount the account
   * @return the gas cost
   */
  default long calculateCodeDelegationResolutionGas(
      final MessageFrame frame, final Account targetAccount) {
    return 0L;
  }

  /**
   * Returns the state gas cost calculator for EIP-8037 multidimensional gas metering.
   *
   * @return the state gas cost calculator (NONE by default)
   */
  default StateGasCostCalculator stateGasCostCalculator() {
    return StateGasCostCalculator.NONE;
  }
}
