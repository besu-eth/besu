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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.frame.CodeReadTracker;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Block-scoped collector of EIP-8025 code reads. Lives for the lifetime of a single block execution
 * and is independent of the EIP-7928 block-access-list pipeline.
 */
public class WitnessCodeTracker implements CodeReadTracker {

  private final Set<Address> codeReads = ConcurrentHashMap.newKeySet();
  private final Set<Address> preStateCodeReads = ConcurrentHashMap.newKeySet();

  @Override
  public void addCodeRead(final Address address) {
    codeReads.add(address);
  }

  @Override
  public void addPreStateCodeRead(final Address address) {
    preStateCodeReads.add(address);
  }

  public Set<Address> getCodeReads() {
    return Collections.unmodifiableSet(codeReads);
  }

  public Set<Address> getPreStateCodeReads() {
    return Collections.unmodifiableSet(preStateCodeReads);
  }
}
