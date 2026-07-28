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
package org.hyperledger.besu.ethereum.p2p.config;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;

/**
 * Resolves which discovery protocol(s) are actually active for a given {@link DiscoveryMode},
 * accounting for the secp256k1-curve fallback: DiscV5 requires a secp256k1 node key and silently
 * falls back to DiscV4-only when the configured key uses a different curve.
 *
 * <p>Both the code that builds the discovery sub-agents and the CLI code that warns about
 * bootnode/mode mismatches must resolve this identically - deriving it independently in each place
 * risks the two silently diverging (e.g. the CLI warning about a protocol that has already fallen
 * back away from running).
 */
public final class DiscoveryModeResolver {

  private DiscoveryModeResolver() {}

  /** The resolved outcome of a {@link DiscoveryMode} plus curve-support check. */
  public record Resolution(boolean v4Enabled, boolean v5Enabled) {}

  /**
   * Resolves which protocol(s) are active for the given mode.
   *
   * @param mode the configured discovery mode
   * @param v5CurveSupported whether the local node key's curve supports DiscV5 (see {@link
   *     #isV5CurveSupported()})
   * @return the resolved V4/V5 active flags
   */
  public static Resolution resolve(final DiscoveryMode mode, final boolean v5CurveSupported) {
    final boolean v5Enabled =
        (mode == DiscoveryMode.BOTH || mode == DiscoveryMode.V5) && v5CurveSupported;
    // Fall back to V4 when V5-only was requested but the node key curve doesn't support DiscV5.
    final boolean v4Enabled =
        mode == DiscoveryMode.BOTH
            || mode == DiscoveryMode.V4
            || (mode == DiscoveryMode.V5 && !v5CurveSupported);
    return new Resolution(v4Enabled, v5Enabled);
  }

  /**
   * Returns whether the local node key's curve supports DiscV5, which requires secp256k1.
   *
   * <p>Reads the process-global {@link SignatureAlgorithmFactory} singleton rather than a
   * per-instance value, since Besu is single-curve-per-process - the configured node key's curve is
   * fixed for the process lifetime.
   *
   * @return {@code true} if the node key curve is secp256k1
   */
  public static boolean isV5CurveSupported() {
    return SECP256K1.CURVE_NAME.equals(SignatureAlgorithmFactory.getInstance().getCurveName());
  }
}
