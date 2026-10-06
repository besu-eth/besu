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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Collections;

public class UncleBlockResult {

  /**
   * Returns an uncle block, which doesn't include transactions or ommers.
   *
   * @param header The uncle block header.
   * @return A BlockResult, generated from the header and empty body.
   */
  public static BlockResult build(final BlockHeader header) {
    final BlockBody body = new BlockBody(Collections.emptyList(), Collections.emptyList());
    final int size = new Block(header, body).getSize();
    return new BlockResult(header, Collections.emptyList(), Collections.emptyList(), size);
  }
}
