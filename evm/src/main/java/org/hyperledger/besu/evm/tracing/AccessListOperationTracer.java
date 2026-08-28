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
package org.hyperledger.besu.evm.tracing;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Table;
import org.apache.tuweni.bytes.Bytes32;

/** The Access List Operation Tracer. */
public class AccessListOperationTracer implements OperationTracer {

  private static final int BALANCE_OPCODE = 0x31;
  private static final int EXTCODESIZE_OPCODE = 0x3B;
  private static final int EXTCODECOPY_OPCODE = 0x3C;
  private static final int EXTCODEHASH_OPCODE = 0x3F;
  private static final int SELFDESTRUCT_OPCODE = 0xFF;
  private static final int CALL_OPCODE = 0xF1;
  private static final int CALLCODE_OPCODE = 0xF2;
  private static final int DELEGATECALL_OPCODE = 0xF4;
  private static final int STATICCALL_OPCODE = 0xFA;

  private final Set<Address> excludedAddresses;
  private final Set<Address> touchedAddresses = new TreeSet<>();

  private Table<Address, Bytes32, Boolean> warmedUpStorage;

  private AccessListOperationTracer(final Set<Address> excludedAddresses) {
    this.excludedAddresses = excludedAddresses;
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    switch (frame.getCurrentOperation().getOpcode()) {
      case BALANCE_OPCODE,
          EXTCODESIZE_OPCODE,
          EXTCODECOPY_OPCODE,
          EXTCODEHASH_OPCODE,
          SELFDESTRUCT_OPCODE -> {
        if (frame.stackSize() >= 1) {
          touch(Words.toAddress(frame.getStackItem(0)));
        }
      }
      case CALL_OPCODE, CALLCODE_OPCODE, DELEGATECALL_OPCODE, STATICCALL_OPCODE -> {
        if (frame.stackSize() >= 5) {
          touch(Words.toAddress(frame.getStackItem(1)));
        }
      }
      default -> {}
    }
  }

  private void touch(final Address address) {
    if (!excludedAddresses.contains(address)) {
      touchedAddresses.add(address);
    }
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final OperationResult operationResult) {
    warmedUpStorage = frame.getWarmedUpStorage();
  }

  /**
   * Get the access list.
   *
   * @return the access list
   */
  public List<AccessListEntry> getAccessList() {
    final List<AccessListEntry> list = new ArrayList<>();
    if (warmedUpStorage != null && !warmedUpStorage.isEmpty()) {
      warmedUpStorage
          .rowMap()
          .forEach(
              (address, storageKeys) ->
                  list.add(
                      new AccessListEntry(
                          address,
                          new ArrayList<>(storageKeys.keySet().stream().sorted().toList()))));
    }
    for (final Address address : touchedAddresses) {
      if (warmedUpStorage == null || !warmedUpStorage.containsRow(address)) {
        list.add(new AccessListEntry(address, List.of()));
      }
    }
    return list;
  }

  /**
   * Create an AccessListOperationTracer.
   *
   * @return the AccessListOperationTracer
   */
  public static AccessListOperationTracer create() {
    return new AccessListOperationTracer(Set.of());
  }

  /**
   * Create an AccessListOperationTracer that omits the given addresses from account-only accesses.
   *
   * @param excludedAddresses addresses never added by account-touching opcodes
   * @return the AccessListOperationTracer
   */
  public static AccessListOperationTracer create(final Set<Address> excludedAddresses) {
    return new AccessListOperationTracer(excludedAddresses);
  }
}
