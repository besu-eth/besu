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
package org.hyperledger.besu.tests.acceptance.dsl.condition.bft;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.condition.Condition;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.bft.BftTransactions;

import java.util.List;

public class AwaitValidatorSetChange implements Condition {

  private final BftTransactions bft;
  private final List<Address> initialSigners;

  public AwaitValidatorSetChange(final List<Address> initialSigners, final BftTransactions bft) {
    this.initialSigners = initialSigners;
    this.bft = bft;
  }

  @Override
  public void verify(final Node node) {
    WaitUtils.waitFor(
        60,
        () ->
            assertThat(node.execute(bft.createGetValidators("latest")))
                .isNotEqualTo(initialSigners));
  }
}
