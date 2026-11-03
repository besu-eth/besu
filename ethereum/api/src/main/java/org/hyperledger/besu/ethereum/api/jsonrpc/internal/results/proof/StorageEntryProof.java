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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class StorageEntryProof {

  private final String key;

  private final UInt256 value;

  private final List<Bytes> storageProof;

  public StorageEntryProof(final String key, final UInt256 value, final List<Bytes> storageProof) {
    this.key = key;
    this.value = value;
    this.storageProof = storageProof;
  }

  @JsonGetter(value = "key")
  public String getKey() {
    return key;
  }

  @JsonGetter(value = "value")
  public String getValue() {
    return Quantity.create(value);
  }

  @JsonGetter(value = "proof")
  public List<String> getStorageProof() {
    return storageProof.stream().map(Bytes::toString).collect(Collectors.toList());
  }
}
