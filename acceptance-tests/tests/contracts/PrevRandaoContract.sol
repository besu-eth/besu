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
pragma solidity >=0.8.19;

// compile with:
// solc PrevRandaoContract.sol --bin --abi --optimize --overwrite -o .
// then create web3j wrappers with:
// web3j generate solidity -b ./generated/PrevRandaoContract.bin -a ./generated/PrevRandaoContract.abi -o ../../../../../ -p org.hyperledger.besu.tests.web3j.generated

// Emits the prevrandao value as an indexed log topic when logPrevRandao() is called.
// Used to verify that all QBFT nodes agree on the prevrandao value during block execution:
// if the proposer and validators use different prevrandao values the log topics differ,
// the bloom filter differs, and the receipts root mismatches — causing the block to be
// rejected by validators and the chain to stall.
contract PrevRandaoContract {
    event PrevRandaoValue(uint256 indexed value);

    uint256 public latestPrevRandao;

    function logPrevRandao() public {
        latestPrevRandao = block.prevrandao;
        emit PrevRandaoValue(block.prevrandao);
    }
}
