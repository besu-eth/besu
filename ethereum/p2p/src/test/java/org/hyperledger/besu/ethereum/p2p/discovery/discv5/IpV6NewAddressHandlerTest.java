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
package org.hyperledger.besu.ethereum.p2p.discovery.discv5;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IpV6NewAddressHandlerTest {
  private @Mock NodeRecordManager nodeRecordManager;
  private @Mock NodeRecord nodeRecord;

  private IpV6NewAddressHandler ipV6NewAddressHandler;

  @Test
  public void testNewAddressWithIpV4Address() {
    ipV6NewAddressHandler = new IpV6NewAddressHandler(nodeRecordManager, false);

    Assertions.assertTrue(
        ipV6NewAddressHandler
            .newAddress(nodeRecord, InetSocketAddress.createUnresolved("10.0.0.2", 123))
            .isEmpty());
    Mockito.verifyNoInteractions(nodeRecordManager, nodeRecord);
  }

  @Test
  public void testNewAddressWithIpV6AddressButAdvertisedIpv6HostIsSet() {
    ipV6NewAddressHandler = new IpV6NewAddressHandler(nodeRecordManager, true);

    Assertions.assertTrue(
        ipV6NewAddressHandler
            .newAddress(nodeRecord, InetSocketAddress.createUnresolved("::1", 123))
            .isEmpty());
    Mockito.verifyNoInteractions(nodeRecordManager, nodeRecord);
  }

  @Test
  public void testNewAddressWithIpV6Address() throws UnknownHostException {
    ipV6NewAddressHandler = new IpV6NewAddressHandler(nodeRecordManager, false);

    DiscoveryPeerV4 localNode = Mockito.mock(DiscoveryPeerV4.class);
    NodeRecord localNodeRecord = Mockito.mock(NodeRecord.class);
    Mockito.when(localNode.getNodeRecord()).thenReturn(Optional.of(localNodeRecord));

    Mockito.when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localNode));

    Optional<NodeRecord> result =
        ipV6NewAddressHandler.newAddress(
            nodeRecord, new InetSocketAddress(Inet6Address.getByName("::1"), 123));

    Mockito.verify(nodeRecordManager).updateAdvertisedIpv6Host("0:0:0:0:0:0:0:1");

    Assertions.assertEquals(Optional.of(localNodeRecord), result);
  }
}
