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
package org.hyperledger.besu.metrics;

/** Enumeration of metrics protocols supported by Besu. */
public enum MetricsProtocol {
  /** Prometheus metrics protocol. */
  PROMETHEUS,
  /** Open telemetry metrics protocol. */
  OPENTELEMETRY,
  /** None metrics protocol. */
  NONE;

  /**
   * Create Metrics Protocol from String representation.
   *
   * @param str the string representation of message protocol
   * @return the metrics protocol
   */
  public static MetricsProtocol fromString(final String str) {
    for (final MetricsProtocol mode : MetricsProtocol.values()) {
      if (mode.name().equalsIgnoreCase(str)) {
        return mode;
      }
    }
    throw new IllegalStateException(String.format("Invalid metrics protocol provided: %s", str));
  }
}
