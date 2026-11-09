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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup.PreBlockCodeReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** A pre-block state of accounts with code, which records the accounts it reads. */
final class RecordingPreBlockCodeReader implements PreBlockCodeReader {

  final List<Address> codeHashReads = new ArrayList<>();
  final List<Address> codeReads = new ArrayList<>();
  private final Map<Address, Bytes> codeByAddress;

  RecordingPreBlockCodeReader(final Map<Address, Bytes> codeByAddress) {
    this.codeByAddress = codeByAddress;
  }

  static RecordingPreBlockCodeReader noPreBlockCode() {
    return new RecordingPreBlockCodeReader(Map.of());
  }

  @Override
  public Optional<Hash> codeHash(final Address address) {
    codeHashReads.add(address);
    return Optional.ofNullable(codeByAddress.get(address)).map(Hash::hash);
  }

  @Override
  public Optional<Bytes> code(final Address address, final Hash codeHash) {
    codeReads.add(address);
    return Optional.ofNullable(codeByAddress.get(address))
        .filter(code -> Hash.hash(code).equals(codeHash));
  }
}
