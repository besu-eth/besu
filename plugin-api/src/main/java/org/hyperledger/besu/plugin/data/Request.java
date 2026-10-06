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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.plugin.Unstable;

import org.apache.tuweni.bytes.Bytes;

/**
 * A request is an operation sent to the Beacon Node
 *
 * @deprecated This type is scheduled for removal in a future release, with no replacement. No
 *     plugin service or data contract returns or accepts it, so it cannot be used by plugins.
 */
@Deprecated(forRemoval = true)
@Unstable
public interface Request {

  /**
   * Retrieves the type of this request.
   *
   * @return The {@link RequestType} representing the type of this request.
   */
  RequestType getType();

  /**
   * Retrieves the data of this request.
   *
   * @return The data {@link Bytes} of this request.
   */
  Bytes getData();
}
