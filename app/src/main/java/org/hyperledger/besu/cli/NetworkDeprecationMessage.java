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
package org.hyperledger.besu.cli;

import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.util.log.FramedLogMessage;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** The Network deprecation message. */
public class NetworkDeprecationMessage {
  private NetworkDeprecationMessage() {}

  /**
   * Generate deprecation message for specified testnet network, using a framed ASCII-art block
   * suitable for plain/pattern log layouts.
   *
   * @param network the network
   * @return the deprecation message for specified network
   */
  public static String generate(final NetworkDefinition network) {
    return generate(network, true);
  }

  /**
   * Generate deprecation message for specified testnet network.
   *
   * @param network the network
   * @param framed true for a framed ASCII-art block (plain/pattern layouts); false for a compact
   *     semicolon-separated single line suitable for structured log formats (GCP, ECS, etc.)
   * @return the deprecation message for specified network
   */
  public static String generate(final NetworkDefinition network, final boolean framed) {
    if (network.getDeprecationDate().isEmpty()) {
      throw new AssertionError("Deprecation date is not set. Cannot print a deprecation message");
    }

    final List<String> lines =
        network
            .getDeprecationMessage()
            .<List<String>>map(msg -> Arrays.asList(msg.split("\n")))
            .orElseGet(
                () ->
                    List.of(
                        network.normalize()
                            + " is deprecated and will be shutdown "
                            + network.getDeprecationDate().get()));

    if (!framed) {
      return lines.stream()
          .map(String::strip)
          .filter(l -> !l.isEmpty())
          .collect(Collectors.joining("; "));
    }

    return network.getDeprecationMessage().isPresent()
        ? FramedLogMessage.generate(lines)
        : FramedLogMessage.generateCentered(lines);
  }
}
