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

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.util.BesuVersionUtils;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VersionProviderTest {

  @Mock private PluginVersionsProvider pluginVersionsProvider;

  @Test
  public void validateEmptyListGenerateBesuInfoVersionOnly() {
    when(pluginVersionsProvider.getPluginVersions()).thenReturn(emptyMap());
    final VersionProvider versionProvider = new VersionProvider(pluginVersionsProvider);
    assertThat(versionProvider.getVersion()).containsOnly(BesuVersionUtils.version());
  }

  @Test
  public void validateVersionListGenerateValidValues() {
    when(pluginVersionsProvider.getPluginVersions())
        .thenReturn(Collections.singletonMap("plugin", "test"));
    final VersionProvider versionProvider = new VersionProvider(pluginVersionsProvider);
    assertThat(versionProvider.getVersion())
        .containsExactly(BesuVersionUtils.version(), "plugin/test");
  }
}
