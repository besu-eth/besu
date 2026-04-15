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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * All {@link Operation} implementations should inherit from this class to get the setting of some
 * members for free.
 */
public abstract class AbstractOperation implements Operation {

  static final Bytes BYTES_ONE = Bytes.of(1);

  private final int opcode;
  private final String name;
  private final int stackItemsConsumed;
  private final int stackItemsProduced;
  private final GasCalculator gasCalculator;

  /**
   * Instantiates a new Abstract operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  protected AbstractOperation(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    this.opcode = opcode & 0xff;
    this.name = name;
    this.stackItemsConsumed = stackItemsConsumed;
    this.stackItemsProduced = stackItemsProduced;
    this.gasCalculator = gasCalculator;
  }

  /**
   * Gets Gas calculator.
   *
   * @return the gas calculator
   */
  protected GasCalculator gasCalculator() {
    return gasCalculator;
  }

  @Override
  public int getOpcode() {
    return opcode;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getStackItemsConsumed() {
    return stackItemsConsumed;
  }

  @Override
  public int getStackItemsProduced() {
    return stackItemsProduced;
  }

  /**
   * Retrieves the {@link Account} at the specified address.
   *
   * <p>If an EIP-7928 Block Access List is active, the address is added to the access list.
   *
   * @param address the account address
   * @param frame the current message execution frame
   * @return the {@link Account}, or {@code null} if it does not exist
   */
  protected Account getAccount(final Address address, final MessageFrame frame) {
    final Account account = frame.getWorldUpdater().get(address);
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(address));
    return account;
  }

  /**
   * Retrieves a mutable view of the account at the specified address.
   *
   * <p>If an EIP-7928 Block Access List is active, the address is added to the access list.
   *
   * @param address the account address
   * @param frame the current message execution frame
   * @return the {@link MutableAccount}, or {@code null} if it does not exist
   */
  protected MutableAccount getMutableAccount(final Address address, final MessageFrame frame) {
    final MutableAccount account = frame.getWorldUpdater().getAccount(address);
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(address));
    return account;
  }

  /**
   * Retrieves a mutable view of the account at the specified address, creating it if it does not
   * exist.
   *
   * <p>If an EIP-7928 Block Access List is active, the address is added to the access list.
   *
   * @param address the account address
   * @param frame the current message execution frame
   * @return the existing or newly created {@link MutableAccount}
   */
  protected MutableAccount getOrCreateAccount(final Address address, final MessageFrame frame) {
    final MutableAccount account = frame.getWorldUpdater().getOrCreate(address);
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(address));
    return account;
  }

  /**
   * Retrieves a mutable view of the sender's account.
   *
   * <p>If an EIP-7928 Block Access List is active, the sender address is added to the access list.
   *
   * @param frame the current message execution frame
   * @return the {@link MutableAccount} for the sender
   */
  protected MutableAccount getSenderAccount(final MessageFrame frame) {
    final MutableAccount account = frame.getWorldUpdater().getSenderAccount(frame);
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(account.getAddress()));
    return account;
  }

  /**
   * Reads a storage slot from the specified account.
   *
   * <p>If an EIP-7928 Block Access List is active, the slot access is recorded for the account.
   *
   * @param account the account whose storage is being accessed
   * @param slotKey the key of the storage slot
   * @param frame the current message execution frame
   * @return the value stored at the specified key
   */
  protected UInt256 getStorageValue(
      final Account account, final UInt256 slotKey, final MessageFrame frame) {
    final UInt256 slotValue = account.getStorageValue(slotKey);
    frame
        .getEip7928AccessList()
        .ifPresent(t -> t.addSlotAccessForAccount(account.getAddress(), slotKey));
    return slotValue;
  }
}
