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
package org.hyperledger.besu.cli.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.util.BesuVersionUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BesuCommandCustomFactoryTest {

  @Mock private PluginVersionsProvider pluginVersionsProvider;

  @BeforeEach
  public void initMocks() {
    when(pluginVersionsProvider.getPluginVersions())
        .thenReturn(ImmutableMap.of("plugin1", "v1", "plugin2", "v2"));
  }

  @Test
  public void testCreateVersionProviderInstance() throws Exception {
    final BesuCommandCustomFactory besuCommandCustomFactory =
        new BesuCommandCustomFactory(pluginVersionsProvider);
    final VersionProvider versionProvider = besuCommandCustomFactory.create(VersionProvider.class);
    assertThat(versionProvider.getVersion())
        .containsExactly(BesuVersionUtils.version(), "plugin1/v1", "plugin2/v2");
  }
}
