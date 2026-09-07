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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.plugin.Unstable;

import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * An internal, unstable modification to a protocol spec at a typed activation point.
 *
 * <p>{@link ProtocolSpecBuilder} is intentionally confined to this explicitly unstable Ethereum
 * extension API. It is not exposed from {@code besu-plugin-api}. A modification is the complete
 * plugin overlay for its era: it remains effective until the next modification in the same
 * activation domain, where it is replaced rather than accumulated.
 *
 * <p>Every modification advertises its activation as an EIP-2124 fork, and only modifications are
 * advertised. A fork that changes no rule is still declared, by repeating the overlay already in
 * force: a modification replaces its predecessor rather than adding to it, so {@link
 * UnaryOperator#identity()} there would drop the rules the era before it established. Identity is
 * right only where the era it opens genuinely has no overlay. A rule change the chain does not
 * advertise as a fork -- a reward era rolling over on a fixed cadence, say -- belongs inside a
 * single modifier rather than in a modification of its own.
 */
@Unstable
public record ProtocolSpecModification(
    ProtocolScheduleActivation activation, UnaryOperator<ProtocolSpecBuilder> modifier) {

  /** Creates and validates a protocol-spec modification. */
  @SuppressWarnings(
      "MethodInputParametersMustBeFinal") // compact record constructors have implicit parameters
  public ProtocolSpecModification {
    Objects.requireNonNull(activation, "activation must not be null");
    Objects.requireNonNull(modifier, "modifier must not be null");
  }
}
