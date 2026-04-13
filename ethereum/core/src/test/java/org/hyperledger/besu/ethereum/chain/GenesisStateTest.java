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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.Account;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

final class GenesisStateTest {

  /** Known RLP encoded bytes of the Olympic Genesis Block. */
  private static final String OLYMPIC_RLP =
      "f901f8f901f3a00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a09178d0f23c965d81f0834a4c72c6253ce6830f4022b1359aaebfc1ecba442d4ea056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302000080832fefd8808080a0000000000000000000000000000000000000000000000000000000000000000088000000000000002ac0c0";

  /** Known Hash of the Olympic Genesis Block. */
  private static final String OLYMPIC_HASH =
      "fd4af92a79c7fc2fd8bf0d342f2e832e1d4f485c85b9152d2039e03bc604fdca";

  private static final String EXPECTED_CODE =
      "0x608060405260043610610116577c01000000000000000000000000000000000000000000000000000000006000350463025e7c278114610158578063173825d91461019e57806320ea8d86146101d15780632f54bf6e146101fb5780633411c81c14610242578063547415251461027b5780637065cb48146102c1578063784547a7146102f45780638b51d13f1461031e5780639ace38c214610348578063a0e67e2b14610415578063a8abe69a1461047a578063b5dc40c3146104ba578063b77bf600146104e4578063ba51a6df146104f9578063c01a8c8414610523578063c64274741461054d578063d74f8edd14610615578063dc8452cd1461062a578063e20056e61461063f578063ee22610b1461067a575b60003411156101565760408051348152905133917fe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c919081900360200190a25b005b34801561016457600080fd5b506101826004803603602081101561017b57600080fd5b50356106a4565b60408051600160a060020a039092168252519081900360200190f35b3480156101aa57600080fd5b50610156600480360360208110156101c157600080fd5b5035600160a060020a03166106cc565b3480156101dd57600080fd5b50610156600480360360208110156101f457600080fd5b503561083c565b34801561020757600080fd5b5061022e6004803603602081101561021e57600080fd5b5035600160a060020a03166108f6565b604080519115158252519081900360200190f35b34801561024e57600080fd5b5061022e6004803603604081101561026557600080fd5b5080359060200135600160a060020a031661090b565b34801561028757600080fd5b506102af6004803603604081101561029e57600080fd5b50803515159060200135151561092b565b60408051918252519081900360200190f35b3480156102cd57600080fd5b50610156600480360360208110156102e457600080fd5b5035600160a060020a0316610997565b34801561030057600080fd5b5061022e6004803603602081101561031757600080fd5b5035610abc565b34801561032a57600080fd5b506102af6004803603602081101561034157600080fd5b5035610b43565b34801561035457600080fd5b506103726004803603602081101561036b57600080fd5b5035610bb2565b6040518085600160a060020a0316600160a060020a031681526020018481526020018060200183151515158152602001828103825284818151815260200191508051906020019080838360005b838110156103d75781810151838201526020016103bf565b50505050905090810190601f1680156104045780820380516001836020036101000a031916815260200191505b509550505050505060405180910390f35b34801561042157600080fd5b5061042a610c70565b60408051602080825283518183015283519192839290830191858101910280838360005b8381101561046657818101518382015260200161044e565b505050509050019250505060405180910390f35b34801561048657600080fd5b5061042a6004803603608081101561049d57600080fd5b508035906020810135906040810135151590606001351515610cd3565b3480156104c657600080fd5b5061042a600480360360208110156104dd57600080fd5b5035610e04565b3480156104f057600080fd5b506102af610f75565b34801561050557600080fd5b506101566004803603602081101561051c57600080fd5b5035610f7b565b34801561052f57600080fd5b506101566004803603602081101561054657600080fd5b5035610ffa565b34801561055957600080fd5b506102af6004803603606081101561057057600080fd5b600160a060020a03823516916020810135918101906060810160408201356401000000008111156105a057600080fd5b8201836020820111156105b257600080fd5b803590602001918460018302840111640100000000831117156105d457600080fd5b91908080601f0160208091040260200160405190810160405280939291908181526020018383808284376000920191909152509295506110c5945050505050565b34801561062157600080fd5b506102af6110e4565b34801561063657600080fd5b506102af6110e9565b34801561064b57600080fd5b506101566004803603604081101561066257600080fd5b50600160a060020a03813581169160200135166110ef565b34801561068657600080fd5b506101566004803603602081101561069d57600080fd5b5035611289565b60038054829081106106b257fe5b600091825260209091200154600160a060020a0316905081565b3330146106d857600080fd5b600160a060020a038116600090815260026020526040902054819060ff16151561070157600080fd5b600160a060020a0382166000908152600260205260408120805460ff191690555b600354600019018110156107d75782600160a060020a031660038281548110151561074957fe5b600091825260209091200154600160a060020a031614156107cf5760038054600019810190811061077657fe5b60009182526020909120015460038054600160a060020a03909216918390811061079c57fe5b9060005260206000200160006101000a815481600160a060020a030219169083600160a060020a031602179055506107d7565b600101610722565b506003805460001901906107eb9082611557565b5060035460045411156108045760035461080490610f7b565b604051600160a060020a038316907f8001553a916ef2f495d26a907cc54d96ed840d7bda71e73194bf5a9df7a76b9090600090a25050565b3360008181526002602052604090205460ff16151561085a57600080fd5b60008281526001602090815260408083203380855292529091205483919060ff16151561088657600080fd5b600084815260208190526040902060030154849060ff16156108a757600080fd5b6000858152600160209081526040808320338085529252808320805460ff191690555187927ff6a317157440607f36269043eb55f1287a5a19ba2216afeab88cd46cbcfb88e991a35050505050565b60026020526000908152604090205460ff1681565b600160209081526000928352604080842090915290825290205460ff1681565b6000805b60055481101561099057838015610958575060008181526020819052604090206003015460ff16155b8061097c575082801561097c575060008181526020819052604090206003015460ff165b15610988576001820191505b60010161092f565b5092915050565b3330146109a357600080fd5b600160a060020a038116600090815260026020526040902054819060ff16156109cb57600080fd5b81600160a060020a03811615156109e157600080fd5b600380549050600101600454603282111580156109fe5750818111155b8015610a0957508015155b8015610a1457508115155b1515610a1f57600080fd5b600160a060020a038516600081815260026020526040808220805460ff1916600190811790915560038054918201815583527fc2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85b01805473ffffffffffffffffffffffffffffffffffffffff191684179055517ff39e6e1eb0edcf53c221607b54b00cd28f3196fed0a24994dc308b8f611b682d9190a25050505050565b600080805b600354811015610b3b5760008481526001602052604081206003805491929184908110610aea57fe5b6000918252602080832090910154600160a060020a0316835282019290925260400190205460ff1615610b1e576001820191505b600454821415610b3357600192505050610b3e565b600101610ac1565b50505b919050565b6000805b600354811015610bac5760008381526001602052604081206003805491929184908110610b7057fe5b6000918252602080832090910154600160a060020a0316835282019290925260400190205460ff1615610ba4576001820191505b600101610b47565b50919050565b6000602081815291815260409081902080546001808301546002808501805487516101009582161595909502600019011691909104601f8101889004880284018801909652858352600160a060020a0390931695909491929190830182828015610c5d5780601f10610c3257610100808354040283529160200191610c5d565b820191906000526020600020905b815481529060010190602001808311610c4057829003601f168201915b5050506003909301549192505060ff1684565b60606003805480602002602001604051908101604052809291908181526020018280548015610cc857602002820191906000526020600020905b8154600160a060020a03168152600190910190602001808311610caa575b505050505090505b90565b606080600554604051908082528060200260200182016040528015610d02578160200160208202803883390190505b5090506000805b600554811015610d8457858015610d32575060008181526020819052604090206003015460ff16155b80610d565750848015610d56575060008181526020819052604090206003015460ff165b15610d7c57808383815181101515610d6a57fe5b60209081029091010152600191909101905b600101610d09565b878703604051908082528060200260200182016040528015610db0578160200160208202803883390190505b5093508790505b86811015610df9578281815181101515610dcd57fe5b9060200190602002015184898303815181101515610de757fe5b60209081029091010152600101610db7565b505050949350505050565b606080600380549050604051908082528060200260200182016040528015610e36578160200160208202803883390190505b5090506000805b600354811015610eee5760008581526001602052604081206003805491929184908110610e6657fe5b6000918252602080832090910154600160a060020a0316835282019290925260400190205460ff1615610ee6576003805482908110610ea157fe5b6000918252602090912001548351600160a060020a0390911690849084908110610ec757fe5b600160a060020a03909216602092830290910190910152600191909101905b600101610e3d565b81604051908082528060200260200182016040528015610f18578160200160208202803883390190505b509350600090505b81811015610f6d578281815181101515610f3657fe5b906020019060200201518482815181101515610f4e57fe5b600160a060020a03909216602092830290910190910152600101610f20565b505050919050565b60055481565b333014610f8757600080fd5b6003548160328211801590610f9c5750818111155b8015610fa757508015155b8015610fb257508115155b1515610fbd57600080fd5b60048390556040805184815290517fa3f1ee9126a074d9326c682f561767f710e927faa811f7a99829d49dc421797a9181900360200190a1505050565b3360008181526002602052604090205460ff16151561101857600080fd5b6000828152602081905260409020548290600160a060020a0316151561103d57600080fd5b60008381526001602090815260408083203380855292529091205484919060ff161561106857600080fd5b6000858152600160208181526040808420338086529252808420805460ff1916909317909255905187927f4a504a94899432a9846e1aa406dceb1bcfd538bb839071d49d1e5e23f5be30ef91a36110be85611289565b5050505050565b60006110d2848484611444565b90506110dd81610ffa565b9392505050565b603281565b60045481565b3330146110fb57600080fd5b600160a060020a038216600090815260026020526040902054829060ff16151561112457600080fd5b600160a060020a038216600090815260026020526040902054829060ff161561114c57600080fd5b82600160a060020a038116151561116257600080fd5b60005b6003548110156111ee5785600160a060020a031660038281548110151561118857fe5b600091825260209091200154600160a060020a031614156111e657846003828154811015156111b357fe5b9060005260206000200160006101000a815481600160a060020a030219169083600160a060020a031602179055506111ee565b600101611165565b50600160a060020a03808616600081815260026020526040808220805460ff1990811690915593881682528082208054909416600117909355915190917f8001553a916ef2f495d26a907cc54d96ed840d7bda71e73194bf5a9df7a76b9091a2604051600160a060020a038516907ff39e6e1eb0edcf53c221607b54b00cd28f3196fed0a24994dc308b8f611b682d90600090a25050505050565b3360008181526002602052604090205460ff1615156112a757600080fd5b60008281526001602090815260408083203380855292529091205483919060ff1615156112d357600080fd5b600084815260208190526040902060030154849060ff16156112f457600080fd5b6112fd85610abc565b156110be576000858152602081815260409182902060038101805460ff19166001908117909155815481830154600280850180548851601f6000199783161561010002979097019091169290920494850187900487028201870190975283815293956113cf95600160a060020a039093169491939283908301828280156113c55780601f1061139a576101008083540402835291602001916113c5565b820191906000526020600020905b8154815290600101906020018083116113a857829003601f168201915b5050505050611534565b156114045760405186907f33e13ecb54c3076d8e8bb8c2881800a4d972b792045ffae98fdf46df365fed7590600090a261143c565b60405186907f526441bb6c1aba3c9a4a6ca1d6545da9c2333c8c48343ef398eb858d72b7923690600090a260038101805460ff191690555b505050505050565b600083600160a060020a038116151561145c57600080fd5b60055460408051608081018252600160a060020a0388811682526020808301898152838501898152600060608601819052878152808452959095208451815473ffffffffffffffffffffffffffffffffffffffff1916941693909317835551600183015592518051949650919390926114dc926002850192910190611580565b50606091909101516003909101805460ff191691151591909117905560058054600101905560405182907fc0ba8fe4b176c1714197d43b9cc6bcf797a4a7461c5fe8d0ef6e184ae7601e5190600090a2509392505050565b6000806040516020840160008287838a8c6187965a03f198975050505050505050565b81548183558181111561157b5760008381526020902061157b9181019083016115fe565b505050565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f106115c157805160ff19168380011785556115ee565b828001600101855582156115ee579182015b828111156115ee5782518255916020019190600101906115d3565b506115fa9291506115fe565b5090565b610cd091905b808211156115fa576000815560010161160456fea165627a7a7230582070d3c680a2cf749f81772e7fffa2883f27a13c65fcfff32190d7585b0c6f0ce40029";

  static class GenesisStateTestArguments implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(DataStorageConfiguration.DEFAULT_FOREST_CONFIG),
          Arguments.of(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  public void createFromJsonWithAllocs(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis1.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getStateRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x92683e6af0f8a932e5fe08c870f2ae9d287e39d4518ec544b0be451f1035fd39"));
    assertThat(header.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getReceiptsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getOmmersHash()).isEqualTo(Hash.EMPTY_LIST_HASH);
    assertThat(header.getExtraData()).isEqualTo(Bytes.EMPTY);
    assertThat(header.getParentHash()).isEqualTo(Hash.ZERO);
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    genesisState.writeStateTo(worldState);
    final Account first =
        worldState.get(Address.fromHexString("0x0000000000000000000000000000000000000001"));
    final Account second =
        worldState.get(Address.fromHexString("0x0000000000000000000000000000000000000002"));
    assertThat(first).isNotNull();
    assertThat(first.getBalance().toLong()).isEqualTo(111111111);
    assertThat(second).isNotNull();
    assertThat(second.getBalance().toLong()).isEqualTo(222222222);
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void createFromJsonNoAllocs(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis2.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getStateRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getReceiptsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getOmmersHash()).isEqualTo(Hash.EMPTY_LIST_HASH);
    assertThat(header.getExtraData()).isEqualTo(Bytes.EMPTY);
    assertThat(header.getParentHash()).isEqualTo(Hash.ZERO);
  }

  private void assertContractInvariants(
      final DataStorageConfiguration dataStorageConfiguration,
      final String sourceFile,
      final String blockHash) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource(sourceFile),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getHash()).isEqualTo(Hash.fromHexString(blockHash));

    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    genesisState.writeStateTo(worldState);
    final Account contract =
        worldState.get(Address.fromHexString("0x3850000000000000000000000000000000000000"));
    assertThat(contract.getCode()).isEqualTo(Bytes.fromHexString(EXPECTED_CODE));
    assertStorageValue(
        contract,
        "c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85d",
        "00000000000000000000000038500f1002084341bf47ee913c4dc2cd92ede0ea");
    assertStorageValue(
        contract,
        "c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f860",
        "000000000000000000000000385ef55e292fa39cf5ffbad99f534294565519ba");
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void createFromJsonWithContract(final DataStorageConfiguration dataStorageConfiguration) {
    assertContractInvariants(
        dataStorageConfiguration,
        "genesis3.json",
        "0xe7fd8db206dcaf066b7c97b8a42a0abc18653613560748557ab44868652a78b6");
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void createFromJsonWithNonce(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesisNonce.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x36750291f1a8429aeb553a790dc2d149d04dbba0ca4cfc7fd5eb12d478117c9f"));
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void encodeOlympicBlock(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis-olympic.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    genesisState.getBlock().writeTo(tmp);
    assertThat(Hex.toHexString(genesisState.getBlock().getHeader().getHash().getBytes().toArray()))
        .isEqualTo(OLYMPIC_HASH);
    assertThat(Hex.toHexString(tmp.encoded().toArray())).isEqualTo(OLYMPIC_RLP);
  }

  private void assertStorageValue(final Account contract, final String key, final String value) {
    assertThat(contract.getStorageValue(UInt256.fromHexString(key)))
        .isEqualTo(UInt256.fromHexString(value));
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void genesisFromShanghai(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis_shanghai.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xfdc41f92053811b877be43e61cab6b0d9ee55501ae2443df0970c753747f12d8"));
    assertThat(header.getGasLimit()).isEqualTo(0x2fefd8);
    assertThat(header.getGasUsed()).isZero();
    assertThat(header.getNumber()).isZero();
    assertThat(header.getReceiptsRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));
    assertThat(header.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getOmmersHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"));
    assertThat(header.getExtraData()).isEqualTo(Bytes.EMPTY);
    assertThat(header.getParentHash()).isEqualTo(Hash.ZERO);

    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    genesisState.writeStateTo(worldState);
    Hash computedStateRoot = worldState.rootHash();
    assertThat(computedStateRoot).isEqualTo(header.getStateRoot());
    assertThat(header.getStateRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x7f5cfe1375a61009a22d24512d18035bc8f855129452fa9c6a6be2ef4e9da7db"));
    final Account first =
        worldState.get(Address.fromHexString("0000000000000000000000000000000000000100"));
    final Account last =
        worldState.get(Address.fromHexString("fb289e2b2b65fb63299a682d000744671c50417b"));
    assertThat(first).isNotNull();
    assertThat(first.getBalance().toLong()).isZero();
    assertThat(first.getCode())
        .isEqualTo(Bytes.fromHexString("0x5f804955600180495560028049556003804955"));
    assertThat(last).isNotNull();
    Wei lastBalance = last.getBalance();
    assertThat(lastBalance).isEqualTo(Wei.fromHexString("0x123450000000000000000"));
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void genesisFromCancun(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis_cancun.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x87846b86c1026fa7d7be2da045716274231de1871065a320659c9b111287c688"));
    assertThat(header.getGasLimit()).isEqualTo(0x2fefd8);
    assertThat(header.getGasUsed()).isZero();
    assertThat(header.getNumber()).isZero();
    assertThat(header.getReceiptsRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));
    assertThat(header.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getOmmersHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"));
    assertThat(header.getExtraData()).isEqualTo(Bytes.EMPTY);
    assertThat(header.getParentHash()).isEqualTo(Hash.ZERO);

    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    genesisState.writeStateTo(worldState);
    Hash computedStateRoot = worldState.rootHash();
    assertThat(computedStateRoot).isEqualTo(header.getStateRoot());
    assertThat(header.getStateRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x7f5cfe1375a61009a22d24512d18035bc8f855129452fa9c6a6be2ef4e9da7db"));
    final Account first =
        worldState.get(Address.fromHexString("0000000000000000000000000000000000000100"));
    final Account last =
        worldState.get(Address.fromHexString("fb289e2b2b65fb63299a682d000744671c50417b"));
    assertThat(first).isNotNull();
    assertThat(first.getBalance().toLong()).isZero();
    assertThat(first.getCode())
        .isEqualTo(Bytes.fromHexString("0x5f804955600180495560028049556003804955"));
    assertThat(last).isNotNull();
    Wei lastBalance = last.getBalance();
    assertThat(lastBalance).isEqualTo(Wei.fromHexString("0x123450000000000000000"));
    assertThat(header.getRequestsHash().isPresent()).isFalse();
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void genesisFromPrague(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis_prague.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x5d2d02fce02d1b7ca635ec91a4fe6f7aa36f9b3997ec4304e8c68d8f6f15d266"));
    assertThat(header.getGasLimit()).isEqualTo(0x2fefd8);
    assertThat(header.getGasUsed()).isZero();
    assertThat(header.getNumber()).isZero();
    assertThat(header.getReceiptsRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));
    assertThat(header.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(header.getOmmersHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"));
    assertThat(header.getExtraData()).isEqualTo(Bytes.EMPTY);
    assertThat(header.getParentHash()).isEqualTo(Hash.ZERO);

    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    genesisState.writeStateTo(worldState);
    Hash computedStateRoot = worldState.rootHash();
    assertThat(computedStateRoot).isEqualTo(header.getStateRoot());
    assertThat(header.getStateRoot())
        .isEqualTo(
            Hash.fromHexString(
                "0x7f5cfe1375a61009a22d24512d18035bc8f855129452fa9c6a6be2ef4e9da7db"));
    final Account first =
        worldState.get(Address.fromHexString("0000000000000000000000000000000000000100"));
    final Account last =
        worldState.get(Address.fromHexString("fb289e2b2b65fb63299a682d000744671c50417b"));
    assertThat(first).isNotNull();
    assertThat(first.getBalance().toLong()).isZero();
    assertThat(first.getCode())
        .isEqualTo(Bytes.fromHexString("0x5f804955600180495560028049556003804955"));
    assertThat(last).isNotNull();
    Wei lastBalance = last.getBalance();
    assertThat(lastBalance).isEqualTo(Wei.fromHexString("0x123450000000000000000"));

    assertThat(header.getRequestsHash().isPresent()).isTrue();
    assertThat(header.getRequestsHash().get())
        .isEqualTo(
            Hash.fromHexString(
                "0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void genesisFromAmsterdam(final DataStorageConfiguration dataStorageConfiguration) {
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis_amsterdam.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getGasLimit()).isEqualTo(0x2fefd8);
    assertThat(header.getGasUsed()).isZero();
    assertThat(header.getNumber()).isZero();
    assertThat(header.getExtraData()).isEqualTo(Bytes.EMPTY);
    assertThat(header.getParentHash()).isEqualTo(Hash.ZERO);

    // Amsterdam-specific: slot number should be present
    assertThat(header.getOptionalSlotNumber()).isPresent();
    assertThat(header.getOptionalSlotNumber().get()).isEqualTo(0x42L);
    assertThat(header.getSlotNumber()).isEqualTo(0x42L);

    // Amsterdam-specific: BAL hash should be present
    assertThat(header.getBalHash()).isPresent();
    assertThat(header.getBalHash().get()).isEqualTo(Hash.EMPTY_BAL_HASH);
  }

  @ParameterizedTest
  @ArgumentsSource(GenesisStateTestArguments.class)
  void genesisSlotNumberNotPresentPreAmsterdam(
      final DataStorageConfiguration dataStorageConfiguration) {
    // Prague genesis should NOT have slot number
    final GenesisState genesisState =
        GenesisState.fromJsonSource(
            dataStorageConfiguration,
            GenesisStateTest.class.getResource("genesis_prague.json"),
            ProtocolScheduleFixture.TESTING_NETWORK);
    final BlockHeader header = genesisState.getBlock().getHeader();
    assertThat(header.getOptionalSlotNumber()).isEmpty();
  }
}
