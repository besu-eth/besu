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
package org.hyperledger.besu.evm.tracing;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.SoftFailureReason;
import org.hyperledger.besu.evm.internal.MemoryEntry;
import org.hyperledger.besu.evm.internal.StorageEntry;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/** Collects information about the current execution frame during a trace. */
public class TraceFrame {
  private final int pc;
  private final Optional<String> opcode;
  private final int opcodeNumber;
  private final long gasRemaining;
  private final OptionalLong gasCost;
  private final long gasRefund;
  private final int depth;
  private final Optional<ExceptionalHaltReason> exceptionalHaltReason;
  private final Address recipient;
  private final Wei value;
  private final Bytes inputData;
  private final Bytes outputData;
  private final Optional<Bytes[]> stack;
  private final Optional<Bytes[]> memory;
  private final Optional<Map<UInt256, UInt256>> storage;
  private final WorldUpdater worldUpdater;
  private final Optional<Bytes> revertReason;
  private final Optional<Map<Address, Wei>> maybeRefunds;
  private final Optional<Code> maybeCode;
  private final int stackItemsProduced;
  private final Optional<Bytes[]> stackPostExecution;
  private final long gasRemainingPostExecution;
  private final boolean virtualOperation;
  private final Optional<MemoryEntry> maybeUpdatedMemory;
  private final Optional<StorageEntry> maybeUpdatedStorage;

  // precompile specific fields
  private final OptionalLong precompiledGasCost;
  private final boolean isPrecompile;
  private final Optional<Address> precompileRecipient;
  private final Optional<Bytes> precompileInputData;
  private final Optional<Bytes> precompileOutputData;

  private final Optional<SoftFailureReason> softFailureReason;
  private final OptionalLong gasAvailableForChildCall;
  private final Optional<Bytes> returnData;

  /** Private constructor - only accessible through Builder */
  private TraceFrame(final Builder builder) {
    this.pc = builder.pc;
    this.opcode = builder.opcode;
    this.opcodeNumber = builder.opcodeNumber;
    this.gasRemaining = builder.gasRemaining;
    this.gasCost = builder.gasCost;
    this.gasRefund = builder.gasRefund;
    this.depth = builder.depth;
    this.exceptionalHaltReason = builder.exceptionalHaltReason;
    this.recipient = builder.recipient;
    this.value = builder.value;
    this.inputData = builder.inputData;
    this.outputData = builder.outputData;
    this.stack = builder.stack;
    this.memory = builder.memory;
    this.storage = builder.storage;
    this.worldUpdater = builder.worldUpdater;
    this.revertReason = builder.revertReason;
    this.maybeRefunds = builder.maybeRefunds;
    this.maybeCode = builder.maybeCode;
    this.stackItemsProduced = builder.stackItemsProduced;
    this.stackPostExecution = builder.stackPostExecution;
    this.gasRemainingPostExecution = builder.gasRemainingPostExecution;
    this.virtualOperation = builder.virtualOperation;
    this.maybeUpdatedMemory = builder.maybeUpdatedMemory;
    this.maybeUpdatedStorage = builder.maybeUpdatedStorage;
    this.precompiledGasCost = builder.precompiledGasCost;
    this.isPrecompile = builder.isPrecompile;
    this.precompileRecipient = builder.precompileRecipient;
    this.precompileInputData = builder.precompileInputData;
    this.precompileOutputData = builder.precompileOutputData;
    this.softFailureReason = builder.softFailureReason;
    this.gasAvailableForChildCall = builder.gasAvailableForChildCall;
    this.returnData = builder.returnData;
  }

  /**
   * Static factory method for creating a new builder
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Static factory method for creating a builder from an existing TraceFrame
   *
   * @param traceFrame the TraceFrame to copy from
   * @return a new Builder instance initialized with the values from the given TraceFrame
   */
  public static Builder from(final TraceFrame traceFrame) {
    return new Builder(traceFrame);
  }

  /** TraceFrame Builder class */
  public static class Builder {
    private int pc;
    private Optional<String> opcode = Optional.empty();
    private int opcodeNumber;
    private long gasRemaining;
    private OptionalLong gasCost = OptionalLong.empty();
    private long gasRefund;
    private int depth;
    private Optional<ExceptionalHaltReason> exceptionalHaltReason = Optional.empty();
    private Address recipient;
    private Wei value;
    private Bytes inputData;
    private Bytes outputData;
    private Optional<Bytes[]> stack = Optional.empty();
    private Optional<Bytes[]> memory = Optional.empty();
    private Optional<Map<UInt256, UInt256>> storage = Optional.empty();
    private WorldUpdater worldUpdater;
    private Optional<Bytes> revertReason = Optional.empty();
    private Optional<Map<Address, Wei>> maybeRefunds = Optional.empty();
    private Optional<Code> maybeCode = Optional.empty();
    private int stackItemsProduced;
    private Optional<Bytes[]> stackPostExecution = Optional.empty();
    private long gasRemainingPostExecution;
    private boolean virtualOperation;
    private Optional<MemoryEntry> maybeUpdatedMemory = Optional.empty();
    private Optional<StorageEntry> maybeUpdatedStorage = Optional.empty();
    private OptionalLong precompiledGasCost = OptionalLong.empty();
    private boolean isPrecompile = false;
    private Optional<Address> precompileRecipient = Optional.empty();
    private Optional<Bytes> precompileInputData = Optional.empty();
    private Optional<Bytes> precompileOutputData = Optional.empty();
    private Optional<SoftFailureReason> softFailureReason = Optional.empty();
    private OptionalLong gasAvailableForChildCall = OptionalLong.empty();
    private Optional<Bytes> returnData = Optional.empty();

    /** Default constructor */
    public Builder() {}

    /**
     * Copy constructor
     *
     * @param traceFrame TraceFrame to copy from.
     */
    public Builder(final TraceFrame traceFrame) {
      this.pc = traceFrame.pc;
      this.opcode = traceFrame.opcode;
      this.opcodeNumber = traceFrame.opcodeNumber;
      this.gasRemaining = traceFrame.gasRemaining;
      this.gasCost = traceFrame.gasCost;
      this.gasRefund = traceFrame.gasRefund;
      this.depth = traceFrame.depth;
      this.exceptionalHaltReason = traceFrame.exceptionalHaltReason;
      this.recipient = traceFrame.recipient;
      this.value = traceFrame.value;
      this.inputData = traceFrame.inputData;
      this.outputData = traceFrame.outputData;
      this.stack = traceFrame.stack;
      this.memory = traceFrame.memory;
      this.storage = traceFrame.storage;
      this.worldUpdater = traceFrame.worldUpdater;
      this.revertReason = traceFrame.revertReason;
      this.maybeRefunds = traceFrame.maybeRefunds;
      this.maybeCode = traceFrame.maybeCode;
      this.stackItemsProduced = traceFrame.stackItemsProduced;
      this.stackPostExecution = traceFrame.stackPostExecution;
      this.gasRemainingPostExecution = traceFrame.gasRemainingPostExecution;
      this.virtualOperation = traceFrame.virtualOperation;
      this.maybeUpdatedMemory = traceFrame.maybeUpdatedMemory;
      this.maybeUpdatedStorage = traceFrame.maybeUpdatedStorage;
      this.precompiledGasCost = traceFrame.precompiledGasCost;
      this.isPrecompile = traceFrame.isPrecompile;
      this.precompileRecipient = traceFrame.precompileRecipient;
      this.precompileInputData = traceFrame.precompileInputData;
      this.precompileOutputData = traceFrame.precompileOutputData;
      this.softFailureReason = traceFrame.softFailureReason;
      this.gasAvailableForChildCall = traceFrame.gasAvailableForChildCall;
      this.returnData = traceFrame.returnData;
    }

    /**
     * Sets the program counter
     *
     * @param pc the program counter value
     * @return this builder instance for method chaining
     */
    public Builder setPc(final int pc) {
      this.pc = pc;
      return this;
    }

    /**
     * Sets the opcode for this trace frame.
     *
     * @param opcode the operation code, or null for no opcode
     * @return this builder instance for method chaining
     */
    public Builder setOpcode(final String opcode) {
      this.opcode = Optional.ofNullable(opcode);
      return this;
    }

    /**
     * Sets the opcode number for this operation.
     *
     * @param opcodeNumber the opcode number
     * @return this builder instance for method chaining
     */
    public Builder setOpcodeNumber(final int opcodeNumber) {
      this.opcodeNumber = opcodeNumber;
      return this;
    }

    /**
     * Sets the remaining gas for this operation.
     *
     * @param gasRemaining the remaining gas in wei
     * @return this builder instance for method chaining
     */
    public Builder setGasRemaining(final long gasRemaining) {
      this.gasRemaining = gasRemaining;
      return this;
    }

    /**
     * Sets the gas cost for this operation.
     *
     * @param gasCost the gas cost in wei
     * @return this builder instance for method chaining
     */
    public Builder setGasCost(final OptionalLong gasCost) {
      this.gasCost = gasCost;
      return this;
    }

    /**
     * Sets the gas refund for this operation.
     *
     * @param gasRefund the gas refund in wei
     * @return this builder instance for method chaining
     */
    public Builder setGasRefund(final long gasRefund) {
      this.gasRefund = gasRefund;
      return this;
    }

    /**
     * Sets the call depth for this operation.
     *
     * @param depth the call depth
     * @return this builder instance for method chaining
     */
    public Builder setDepth(final int depth) {
      this.depth = depth;
      return this;
    }

    /**
     * Sets the exceptional halt reason for this operation.
     *
     * @param exceptionalHaltReason the exceptional halt reason, or null for no halt reason
     * @return this builder instance for method chaining
     */
    public Builder setExceptionalHaltReason(
        final Optional<ExceptionalHaltReason> exceptionalHaltReason) {
      this.exceptionalHaltReason = exceptionalHaltReason;
      return this;
    }

    /**
     * Sets the soft failure reason for this operation.
     *
     * @param softFailureReason the soft failure reason, or null for no soft failure reason
     * @return this builder instance for method chaining
     */
    public Builder setSoftFailureReason(final Optional<SoftFailureReason> softFailureReason) {
      this.softFailureReason = softFailureReason;
      return this;
    }

    /**
     * Sets the recipient address for this operation.
     *
     * @param recipient the recipient address
     * @return this builder instance for method chaining
     */
    public Builder setRecipient(final Address recipient) {
      this.recipient = recipient;
      return this;
    }

    /**
     * Sets the value transferred in this operation.
     *
     * @param value the value in wei
     * @return this builder instance for method chaining
     */
    public Builder setValue(final Wei value) {
      this.value = value;
      return this;
    }

    /**
     * Sets the input data for this operation.
     *
     * @param inputData the input data as a byte array
     * @return this builder instance for method chaining
     */
    public Builder setInputData(final Bytes inputData) {
      this.inputData = inputData;
      return this;
    }

    /**
     * Sets the output data for this operation.
     *
     * @param outputData the output data as a byte array
     * @return this builder instance for method chaining
     */
    public Builder setOutputData(final Bytes outputData) {
      this.outputData = outputData;
      return this;
    }

    /**
     * Sets the stack for this operation.
     *
     * @param stack the stack as an array of byte arrays, or null for no stack
     * @return this builder instance for method chaining
     */
    public Builder setStack(final Optional<Bytes[]> stack) {
      this.stack = stack;
      return this;
    }

    /**
     * Sets the memory for this operation.
     *
     * @param memory the memory as an optional array of byte arrays
     * @return this builder instance for method chaining
     */
    public Builder setMemory(final Optional<Bytes[]> memory) {
      this.memory = memory;
      return this;
    }

    /**
     * Sets the storage for this operation.
     *
     * @param storage the storage as an optional map of UInt256 keys and values
     * @return this builder instance for method chaining
     */
    public Builder setStorage(final Optional<Map<UInt256, UInt256>> storage) {
      this.storage = storage;
      return this;
    }

    /**
     * Sets the world updater for this operation.
     *
     * @param worldUpdater the world updater
     * @return this builder instance for method chaining
     */
    public Builder setWorldUpdater(final WorldUpdater worldUpdater) {
      this.worldUpdater = worldUpdater;
      return this;
    }

    /**
     * Sets the revert reason for this operation.
     *
     * @param revertReason the revert reason as a byte array, or null for no revert reason
     * @return this builder instance for method chaining
     */
    public Builder setRevertReason(final Optional<Bytes> revertReason) {
      this.revertReason = revertReason;
      return this;
    }

    /**
     * Sets the maybe refunds for this operation.
     *
     * @param maybeRefunds the maybe refunds as an optional map of Address keys and Wei values
     * @return this builder instance for method chaining
     */
    public Builder setMaybeRefunds(final Optional<Map<Address, Wei>> maybeRefunds) {
      this.maybeRefunds = maybeRefunds;
      return this;
    }

    /**
     * Sets the maybe code for this operation.
     *
     * @param maybeCode the maybe code, or null for no code
     * @return this builder instance for method chaining
     */
    public Builder setMaybeCode(final Optional<Code> maybeCode) {
      this.maybeCode = maybeCode;
      return this;
    }

    /**
     * Sets the number of stack items produced by this operation.
     *
     * @param stackItemsProduced the number of stack items produced
     * @return this builder instance for method chaining
     */
    public Builder setStackItemsProduced(final int stackItemsProduced) {
      this.stackItemsProduced = stackItemsProduced;
      return this;
    }

    /**
     * Sets the stack after execution for this operation.
     *
     * @param stackPostExecution the stack after execution as an array of byte arrays, or null for
     *     no stack
     * @return this builder instance for method chaining
     */
    public Builder setStackPostExecution(final Optional<Bytes[]> stackPostExecution) {
      this.stackPostExecution = stackPostExecution;
      return this;
    }

    /**
     * Sets the gas remaining after execution for this operation.
     *
     * @param gasRemainingPostExecution the gas remaining after execution in wei
     * @return this builder instance for method chaining
     */
    public Builder setGasRemainingPostExecution(final long gasRemainingPostExecution) {
      this.gasRemainingPostExecution = gasRemainingPostExecution;
      return this;
    }

    /**
     * Sets whether this operation is a virtual operation (not part of the original bytecode).
     *
     * @param virtualOperation true if this is a virtual operation, false otherwise
     * @return this builder instance for method chaining
     */
    public Builder setVirtualOperation(final boolean virtualOperation) {
      this.virtualOperation = virtualOperation;
      return this;
    }

    /**
     * Sets the maybe updated memory entry for this operation.
     *
     * @param maybeUpdatedMemory the maybe updated memory entry, or null for no update
     * @return this builder instance for method chaining
     */
    public Builder setMaybeUpdatedMemory(final Optional<MemoryEntry> maybeUpdatedMemory) {
      this.maybeUpdatedMemory = maybeUpdatedMemory;
      return this;
    }

    /**
     * Sets the maybe updated storage entry for this operation.
     *
     * @param maybeUpdatedStorage the maybe updated storage entry, or null for no update
     * @return this builder instance for method chaining
     */
    public Builder setMaybeUpdatedStorage(final Optional<StorageEntry> maybeUpdatedStorage) {
      this.maybeUpdatedStorage = maybeUpdatedStorage;
      return this;
    }

    /**
     * Sets the precompiled gas cost for this operation.
     *
     * @param precompiledGasCost the precompiled gas cost in wei
     * @return this builder instance for method chaining
     */
    public Builder setPrecompiledGasCost(final long precompiledGasCost) {
      this.precompiledGasCost = OptionalLong.of(precompiledGasCost);
      return this;
    }

    /**
     * Sets whether this operation is a precompile call.
     *
     * @param isPrecompile true if this is a precompile call, false otherwise
     * @return this builder instance for method chaining
     */
    public Builder setIsPrecompile(final boolean isPrecompile) {
      this.isPrecompile = isPrecompile;
      return this;
    }

    /**
     * Sets the precompile recipient address for this operation.
     *
     * @param precompileRecipient the precompile recipient address, or null for no recipient
     * @return this builder instance for method chaining
     */
    public Builder setPrecompileRecipient(final Address precompileRecipient) {
      this.precompileRecipient = Optional.ofNullable(precompileRecipient);
      return this;
    }

    /**
     * Sets the precompile input data for this operation.
     *
     * @param precompileInputData the precompile input data as a byte array, or null for no input
     *     data
     * @return this builder instance for method chaining
     */
    public Builder setPrecompileInputData(final Bytes precompileInputData) {
      this.precompileInputData = Optional.ofNullable(precompileInputData);
      return this;
    }

    /**
     * Sets the precompile output data for this operation.
     *
     * @param precompileOutputData the precompile output data as a byte array, or null for no output
     *     data
     * @return this builder instance for method chaining
     */
    public Builder setPrecompileOutputData(final Bytes precompileOutputData) {
      this.precompileOutputData = Optional.ofNullable(precompileOutputData);
      return this;
    }

    /**
     * Convenience method to set all precompile IO data at once This also sets isPrecompile to true.
     *
     * @param recipient the precompile recipient address, or null for no recipient
     * @param input the input to the precompile, or null for no input data
     * @param output the output from the precompile, or null for no output data
     * @return this builder instance for method chaining
     */
    public Builder setPrecompileIOData(
        final Address recipient, final Bytes input, final Bytes output) {
      this.isPrecompile = true;
      this.precompileRecipient = Optional.ofNullable(recipient);
      this.precompileInputData = Optional.ofNullable(input);
      this.precompileOutputData = Optional.ofNullable(output);
      return this;
    }

    /**
     * Sets the gas available for child call, if applicable.
     *
     * @param gasAvailableForChildCall the gas available for child call
     * @return this builder instance for method chaining
     */
    public Builder setGasAvailableForChildCall(final OptionalLong gasAvailableForChildCall) {
      this.gasAvailableForChildCall = gasAvailableForChildCall;
      return this;
    }

    /**
     * Sets the return data buffer captured after the opcode executed.
     *
     * @param returnData the return data buffer contents
     * @return this builder instance for method chaining
     */
    public Builder setReturnData(final Optional<Bytes> returnData) {
      this.returnData = returnData;
      return this;
    }

    /**
     * Builds the TraceFrame instance.
     *
     * @return the constructed TraceFrame
     */
    public TraceFrame build() {
      return new TraceFrame(this);
    }
  }

  /**
   * current program counter
   *
   * @return current program counter
   */
  public int getPc() {
    return pc;
  }

  /**
   * current opcode
   *
   * @return current opcode
   */
  public String getOpcode() {
    return opcode.orElse("");
  }

  /**
   * number of the opcode
   *
   * @return number of the opcode
   */
  public int getOpcodeNumber() {
    return opcodeNumber;
  }

  /**
   * how much gas remains
   *
   * @return unused portion of gas
   */
  public long getGasRemaining() {
    return gasRemaining;
  }

  /**
   * gas cost
   *
   * @return gas cost
   */
  public OptionalLong getGasCost() {
    return gasCost;
  }

  /**
   * gas to be refunded
   *
   * @return amount of gas to be refunded
   */
  public long getGasRefund() {
    return gasRefund;
  }

  /**
   * frame depth
   *
   * @return frame depth
   */
  public int getDepth() {
    return depth;
  }

  /**
   * If halted, why?
   *
   * @return reason for halting
   */
  public Optional<ExceptionalHaltReason> getExceptionalHaltReason() {
    return exceptionalHaltReason;
  }

  /**
   * the address being sent to
   *
   * @return the address being sent to
   */
  public Address getRecipient() {
    return recipient;
  }

  /**
   * amount being sent
   *
   * @return amount being sent
   */
  public Wei getValue() {
    return value;
  }

  /**
   * data to execute over
   *
   * @return data to execute over
   */
  public Bytes getInputData() {
    return this.inputData;
  }

  /**
   * computed data to return
   *
   * @return computed data to return
   */
  public Bytes getOutputData() {
    return outputData;
  }

  /**
   * what is on the stack
   *
   * @return what is on the stack
   */
  public Optional<Bytes[]> getStack() {
    return stack;
  }

  /**
   * what is in memory
   *
   * @return what is in memory
   */
  public Optional<Bytes[]> getMemory() {
    return memory;
  }

  /**
   * data storage slots and values
   *
   * @return data storage slots and values
   */
  public Optional<Map<UInt256, UInt256>> getStorage() {
    return storage;
  }

  /**
   * handle to the state updater
   *
   * @return handle to the state updater
   */
  public WorldUpdater getWorldUpdater() {
    return worldUpdater;
  }

  /**
   * reason for reverting transaction
   *
   * @return reason for reverting transaction
   */
  public Optional<Bytes> getRevertReason() {
    return revertReason;
  }

  /**
   * map of addresses getting refunds, maybe
   *
   * @return map of addresses getting refunds, maybe
   */
  public Optional<Map<Address, Wei>> getMaybeRefunds() {
    return maybeRefunds;
  }

  /**
   * evm bytecode, maybe
   *
   * @return evm bytecode, maybe
   */
  public Optional<Code> getMaybeCode() {
    return maybeCode;
  }

  /**
   * what was put on the stack
   *
   * @return what was put on the stack
   */
  public int getStackItemsProduced() {
    return stackItemsProduced;
  }

  /**
   * stack content after execution
   *
   * @return stack content after execution
   */
  public Optional<Bytes[]> getStackPostExecution() {
    return stackPostExecution;
  }

  /**
   * gas left over after running
   *
   * @return gas left over after running
   */
  public long getGasRemainingPostExecution() {
    return gasRemainingPostExecution;
  }

  /**
   * if this is a virtual operation or not
   *
   * @return if this is a virtual operation or not
   */
  public boolean isVirtualOperation() {
    return virtualOperation;
  }

  /**
   * if memory was updated, return the entry
   *
   * @return modified memory entry, maybe
   */
  public Optional<MemoryEntry> getMaybeUpdatedMemory() {
    return maybeUpdatedMemory;
  }

  /**
   * if storage was updated, return it
   *
   * @return updated storage, maybe
   */
  public Optional<StorageEntry> getMaybeUpdatedStorage() {
    return maybeUpdatedStorage;
  }

  /**
   * gas cost
   *
   * @return gas cost
   */
  public OptionalLong getPrecompiledGasCost() {
    return precompiledGasCost;
  }

  /**
   * whether the current operation is a precompile
   *
   * @return true if precompile, else false
   */
  public boolean isPrecompile() {
    return isPrecompile;
  }

  /**
   * The address of the precompile being called.
   *
   * @return the address of the precompile being called
   */
  public Optional<Address> getPrecompileRecipient() {
    return precompileRecipient;
  }

  /**
   * Input data sent to the precompile.
   *
   * @return input data sent to the precompile
   */
  public Optional<Bytes> getPrecompileInputData() {
    return precompileInputData;
  }

  /**
   * Output data returned from the precompile.
   *
   * @return output data returned from the precompile
   */
  public Optional<Bytes> getPrecompileOutputData() {
    return precompileOutputData;
  }

  /**
   * Returns optional Soft Failure Reason.
   *
   * @return Optional Soft Failure Reason
   */
  public Optional<SoftFailureReason> getSoftFailureReason() {
    return softFailureReason;
  }

  /**
   * If the operation involved a call, returns the gas available for the child call.
   *
   * @return OptionalLong containing the gas available for the child call, or empty if not
   *     applicable.
   */
  public OptionalLong getGasAvailableForChildCall() {
    return gasAvailableForChildCall;
  }

  /**
   * Return data buffer at the time this opcode was traced.
   *
   * @return the return data buffer, or empty if not captured
   */
  public Optional<Bytes> getReturnData() {
    return returnData;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("pc", pc)
        .add("opcode", opcode)
        .add("gasRemaining", gasRemaining)
        .add("gasCost", gasCost)
        .add("depth", depth)
        .add("exceptionalHaltReason", exceptionalHaltReason)
        .add("stack", stack)
        .add("memory", memory)
        .add("storage", storage)
        .add("returnData", returnData)
        .toString();
  }
}
