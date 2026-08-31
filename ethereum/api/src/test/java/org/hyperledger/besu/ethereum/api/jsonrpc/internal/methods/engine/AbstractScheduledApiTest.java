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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.EXPERIMENTAL_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.LONDON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PARIS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.util.Optional;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AbstractScheduledApiTest extends TrustedSetupClassLoaderExtension {

  protected final ScheduledProtocolSpec.Hardfork londonHardfork =
      new ScheduledProtocolSpec.Hardfork("London", 0);
  protected final ScheduledProtocolSpec.Hardfork parisHardfork =
      new ScheduledProtocolSpec.Hardfork("Paris", 10);
  protected final ScheduledProtocolSpec.Hardfork shanghaiHardfork =
      new ScheduledProtocolSpec.Hardfork("Shanghai", 20);
  protected final ScheduledProtocolSpec.Hardfork cancunHardfork =
      new ScheduledProtocolSpec.Hardfork("Cancun", 30);
  protected final ScheduledProtocolSpec.Hardfork pragueHardfork =
      new ScheduledProtocolSpec.Hardfork("Prague", 50);
  protected final ScheduledProtocolSpec.Hardfork osakaHardfork =
      new ScheduledProtocolSpec.Hardfork("Osaka", 60);
  protected final ScheduledProtocolSpec.Hardfork amsterdamHardfork =
      new ScheduledProtocolSpec.Hardfork("Amsterdam", 70);
  protected final ScheduledProtocolSpec.Hardfork bogotaHardfork =
      new ScheduledProtocolSpec.Hardfork("Bogota", 80);
  protected final ScheduledProtocolSpec.Hardfork experimentalHardfork =
      new ScheduledProtocolSpec.Hardfork("ExperimentalEips", 90);

  @Mock protected ProtocolSchedule protocolSchedule;

  static class HardforkMatcher implements ArgumentMatcher<Predicate<ScheduledProtocolSpec>> {
    private final ScheduledProtocolSpec.Hardfork fork;
    private final ScheduledProtocolSpec spec;

    public HardforkMatcher(final ScheduledProtocolSpec.Hardfork hardfork) {
      this.fork = hardfork;
      this.spec = mock(ScheduledProtocolSpec.class);
      lenient().when(spec.fork()).thenReturn(fork);
    }

    @Override
    public boolean matches(final Predicate<ScheduledProtocolSpec> value) {
      if (value == null) {
        return false;
      }
      return value.test(spec);
    }
  }

  @BeforeEach
  public void before() {
    lenient()
        .when(protocolSchedule.milestoneFor((LONDON)))
        .thenReturn(Optional.of(londonHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(PARIS))
        .thenReturn(Optional.of(parisHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(SHANGHAI))
        .thenReturn(Optional.of(shanghaiHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(CANCUN))
        .thenReturn(Optional.of(cancunHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(PRAGUE))
        .thenReturn(Optional.of(pragueHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(OSAKA))
        .thenReturn(Optional.of(osakaHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(AMSTERDAM))
        .thenReturn(Optional.of(amsterdamHardfork.milestone()));
    lenient()
        .when(protocolSchedule.milestoneFor(EXPERIMENTAL_EIPS))
        .thenReturn(Optional.of(experimentalHardfork.milestone()));
  }
}
