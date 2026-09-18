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

import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.TransientStorageKey;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.tuweni.bytes.Bytes32;

/** The Access List Operation Tracer. */
public class AccessListOperationTracer implements OperationTracer {

  private UndoSet<TransientStorageKey> warmedUpStorage;

  /** Default constructor. */
  private AccessListOperationTracer() {
    super();
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
    if (warmedUpStorage == null || warmedUpStorage.isEmpty()) {
      return List.of();
    }
    final HashMap<Address, List<Bytes32>> storageKeysByAddress = new HashMap<>();
    warmedUpStorage.forEach(
        transientStorageKey -> {
          final Address address = transientStorageKey.address();
          final Bytes32 slot = transientStorageKey.slot();
          storageKeysByAddress.computeIfAbsent(address, _ -> new ArrayList<>()).add(slot);
        });
    final List<AccessListEntry> list = new ArrayList<>(storageKeysByAddress.size());
    storageKeysByAddress.forEach(
        (address, storageKeys) ->
            list.add(new AccessListEntry(address, storageKeys.stream().sorted().toList())));
    return list;
  }

  /**
   * Create an AccessListOperationTracer.
   *
   * @return the AccessListOperationTracer
   */
  public static AccessListOperationTracer create() {
    return new AccessListOperationTracer();
  }
}
