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
package org.hyperledger.besu.ethereum.core;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.RequestType;

import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.tuweni.bytes.Bytes;

public record Request(RequestType type, Bytes data)
    implements org.hyperledger.besu.plugin.data.Request {

  @JsonCreator
  public static Request fromBytes(final Bytes bytes) {
    checkArgument(!bytes.isEmpty(), "Request cannot be empty");

    final RequestType type = RequestType.of(bytes.get(0));
    final Bytes data = bytes.slice(1);

    checkArgument(!data.isEmpty(), "Request must be at least 1 byte");

    return new Request(type, data);
  }

  @Override
  public RequestType getType() {
    return type();
  }

  @Override
  public Bytes getData() {
    return data();
  }

  /**
   * Gets the serialized form of the concatenated type and data.
   *
   * @return the serialized request as a byte.
   */
  @JsonValue
  public Bytes getEncodedRequest() {
    return Bytes.concatenate(Bytes.of(getType().getSerializedType()), getData());
  }

  /**
   * Converts a list of request to the protocol canonical form: elements of the list MUST be ordered
   * by request_type in ascending order. Elements with empty request_data MUST be excluded from the
   * list.
   *
   * @param requests list of requests
   * @return protocol canonical request list
   */
  public static List<Request> asCanonicalList(final List<Request> requests) {
    return requests.stream()
        .sorted(Comparator.comparing(Request::getType))
        .filter(r -> !r.getData().isEmpty())
        .toList();
  }
}
