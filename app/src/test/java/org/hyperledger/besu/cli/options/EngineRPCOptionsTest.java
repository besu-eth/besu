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
package org.hyperledger.besu.cli.options;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidationMode;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidator;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.LenientInclusionListValidator;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.StrictInclusionListValidator;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class EngineRPCOptionsTest {

  @Test
  void defaultInclusionListValidationModeIsStrict() {
    final EngineRPCOptions options = new EngineRPCOptions();
    final EngineRPCConfiguration config = options.toDomainObject();
    assertThat(config.inclusionListValidationMode()).isEqualTo(InclusionListValidationMode.STRICT);
  }

  @Test
  void parsesStrictValidationMode() {
    final EngineRPCOptions options = parseOptions("--engine-inclusion-list-validation-mode=STRICT");
    final EngineRPCConfiguration config = options.toDomainObject();
    assertThat(config.inclusionListValidationMode()).isEqualTo(InclusionListValidationMode.STRICT);
  }

  @Test
  void parsesLenientValidationMode() {
    final EngineRPCOptions options =
        parseOptions("--engine-inclusion-list-validation-mode=LENIENT");
    final EngineRPCConfiguration config = options.toDomainObject();
    assertThat(config.inclusionListValidationMode()).isEqualTo(InclusionListValidationMode.LENIENT);
  }

  @Test
  void strictModeCreatesStrictValidatorThatRejects() {
    final EngineRPCOptions options = parseOptions("--engine-inclusion-list-validation-mode=STRICT");
    final InclusionListValidationMode mode = options.toDomainObject().inclusionListValidationMode();
    final InclusionListValidator validator = mode.createValidator();

    assertThat(validator).isInstanceOf(StrictInclusionListValidator.class);

    // Strict validator rejects when IL transaction is missing from payload
    final Bytes tx = Bytes.fromHexString("0x1234");
    final var result = validator.validate(List.of(), List.of(tx));
    assertThat(result.isValid()).isFalse();
  }

  @Test
  void lenientModeCreatesLenientValidatorThatAccepts() {
    final EngineRPCOptions options =
        parseOptions("--engine-inclusion-list-validation-mode=LENIENT");
    final InclusionListValidationMode mode = options.toDomainObject().inclusionListValidationMode();
    final InclusionListValidator validator = mode.createValidator();

    assertThat(validator).isInstanceOf(LenientInclusionListValidator.class);

    // Lenient validator accepts even when IL transaction is missing from payload
    final Bytes tx = Bytes.fromHexString("0x1234");
    final var result = validator.validate(List.of(), List.of(tx));
    assertThat(result.isValid()).isTrue();
  }

  @Test
  void toInclusionListConfigurationPreservesValidationMode() {
    final EngineRPCOptions options =
        parseOptions("--engine-inclusion-list-validation-mode=LENIENT");
    final InclusionListConfiguration ilConfig =
        options.toDomainObject().toInclusionListConfiguration();
    assertThat(ilConfig.validationMode()).isEqualTo(InclusionListValidationMode.LENIENT);
  }

  @Test
  void defaultToInclusionListConfigurationHasStrictMode() {
    final EngineRPCOptions options = new EngineRPCOptions();
    final InclusionListConfiguration ilConfig =
        options.toDomainObject().toInclusionListConfiguration();
    assertThat(ilConfig.validationMode()).isEqualTo(InclusionListValidationMode.STRICT);
  }

  private EngineRPCOptions parseOptions(final String... args) {
    final EngineRPCOptions options = new EngineRPCOptions();
    new CommandLine(options).parseArgs(args);
    return options;
  }
}
