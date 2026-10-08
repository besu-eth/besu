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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class EvmToolCommandSelfDestructTest {

  private static final String PRAGUE_STATE_ROOT_ACCOUNT_DELETED =
      "0x7e0022d781bcd8c6066632ac935341c9f03366b84ad12ded43d7cc7d6efc39de";
  private static final String AMSTERDAM_STATE_ROOT_BALANCE_KEPT =
      "0x9845b11f84af22cd9ce73b0f6ec434e7cbca62b313af7a30e058eb7724225a0b";

  @Test
  void selfDestructDeletesAccountBeforeAmsterdam() {
    assertThat(runSelfDestructingCreation("prague").getString("stateRoot"))
        .isEqualTo(PRAGUE_STATE_ROOT_ACCOUNT_DELETED);
  }

  @Test
  void selfDestructPreservesBalanceFromAmsterdam() {
    assertThat(runSelfDestructingCreation("amsterdam").getString("stateRoot"))
        .isEqualTo(AMSTERDAM_STATE_ROOT_BALANCE_KEPT);
  }

  private static JsonObject runSelfDestructingCreation(final String fork) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // Init code ADDRESS SELFDESTRUCT: the new contract sends its 1000 wei to itself.
    new EvmToolCommand()
        .execute(
            new ByteArrayInputStream(new byte[0]),
            new PrintWriter(baos, true, UTF_8),
            new String[] {
              "--notime",
              "--json",
              "--fork",
              fork,
              "--prestate",
              EvmToolCommandSelfDestructTest.class
                  .getResource("selfdestruct-funded-sender.json")
                  .getPath(),
              "--sender",
              "0x00000000000000000000000000000000000000aa",
              "--receiver",
              "0x00000000000000000000000000000000000000aa",
              "--create",
              "--value",
              "1000",
              "--code",
              "30ff"
            });
    return new JsonObject(baos.toString(UTF_8).lines().reduce((first, last) -> last).orElseThrow());
  }
}
