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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Covers the pure {@link DiscoveryModeResolver#resolve} truth table. Deliberately does not cover
 * {@link DiscoveryModeResolver#isV5CurveSupported()} - it reads a JVM-wide static singleton
 * (SignatureAlgorithmFactory), and toggling that mid-test-run poisons other tests in the same
 * worker (see CompositePeerDiscoveryAgentFactoryTest's class javadoc for the same caveat).
 */
class DiscoveryModeResolverTest {

  @Test
  void both_withSecp256k1_enablesBoth() {
    final DiscoveryModeResolver.Resolution r =
        DiscoveryModeResolver.resolve(DiscoveryMode.BOTH, true);
    assertThat(r.v4Enabled()).isTrue();
    assertThat(r.v5Enabled()).isTrue();
  }

  @Test
  void both_withoutSecp256k1_fallsBackToV4Only() {
    final DiscoveryModeResolver.Resolution r =
        DiscoveryModeResolver.resolve(DiscoveryMode.BOTH, false);
    assertThat(r.v4Enabled()).isTrue();
    assertThat(r.v5Enabled()).isFalse();
  }

  @Test
  void v4_withSecp256k1_enablesV4Only() {
    final DiscoveryModeResolver.Resolution r =
        DiscoveryModeResolver.resolve(DiscoveryMode.V4, true);
    assertThat(r.v4Enabled()).isTrue();
    assertThat(r.v5Enabled()).isFalse();
  }

  @Test
  void v4_withoutSecp256k1_enablesV4Only() {
    final DiscoveryModeResolver.Resolution r =
        DiscoveryModeResolver.resolve(DiscoveryMode.V4, false);
    assertThat(r.v4Enabled()).isTrue();
    assertThat(r.v5Enabled()).isFalse();
  }

  @Test
  void v5_withSecp256k1_enablesV5Only() {
    final DiscoveryModeResolver.Resolution r =
        DiscoveryModeResolver.resolve(DiscoveryMode.V5, true);
    assertThat(r.v4Enabled()).isFalse();
    assertThat(r.v5Enabled()).isTrue();
  }

  @Test
  void v5_withoutSecp256k1_fallsBackToV4Only() {
    // The exact case that used to produce a misleading "add ENR bootnodes" CLI warning: V5 was
    // requested but the curve doesn't support it, so V4 silently runs instead.
    final DiscoveryModeResolver.Resolution r =
        DiscoveryModeResolver.resolve(DiscoveryMode.V5, false);
    assertThat(r.v4Enabled()).isTrue();
    assertThat(r.v5Enabled()).isFalse();
  }
}
