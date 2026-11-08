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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.exception.StorageException;

/** The code of a stored account is missing from the world state, a fault of the local storage. */
public class MissingCodeException extends StorageException {

  private final Address address;
  private final Hash codeHash;

  public MissingCodeException(final Address address, final Hash codeHash) {
    super(String.format("Missing code for account %s with code hash %s", address, codeHash));
    this.address = address;
    this.codeHash = codeHash;
  }

  public Address getAddress() {
    return address;
  }

  public Hash getCodeHash() {
    return codeHash;
  }
}
