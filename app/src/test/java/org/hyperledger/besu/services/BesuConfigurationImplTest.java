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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BesuConfigurationImplTest {

  @Test
  void getBesuVersion_returnsNonEmptyString() {
    BesuConfigurationImpl config = new BesuConfigurationImpl();
    String version = config.getBesuVersion();
    assertThat(version).isNotNull();
    assertThat(version).isNotEmpty();
  }

  @Test
  void getBesuCommitHash_returnsNonEmptyString() {
    BesuConfigurationImpl config = new BesuConfigurationImpl();
    String commit = config.getBesuCommitHash();
    assertThat(commit).isNotNull();
    assertThat(commit).isNotEmpty();
  }
}
