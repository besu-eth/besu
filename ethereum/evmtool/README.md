EVM Tool
========

EVM Tool is a stand alone EVM executor and test execution tool. The
principal purpose of the tool is for testing and validation of the EVM
and enclosing data structures.

Using EVM Tool in execution-specification-tests
-----------------------------------------------

### Building Execution Tests on macOS

Current as of 26 Jun 2024.

MacOS users will typically encounter two problems,one relating to the
version of Python used and one relating to zsh.

Homebrew will only install the most recent version of Python
as `python3`, and that is 3.11. The execution tests require 3.10. The
solution is to use a 3.10 version of python to set up the virtual
environment.

```zsh
python3.10 -m venv ./venv/
```

Zsh requires braces to be escaped in the command line, so the step to
install python packages needs to escape the brackets

```zsh
pip install -e .\[docs,lint,test\]
```

An all-in-one script, using homebrew, would look like

```zsh
brew install ethereum solidity
git clone https://github.com/ethereum/execution-spec-tests
cd execution-spec-tests
python3 -m venv ./venv/
source ./venv/bin/activate
pip install -e .\[docs,lint,test\]
```

### Building EvmTool on macOS

First you need a Java 25+ JDK installed, if not already installed.

It is recommended you install [SDKMAN](https://sdkman.io/install) to
manage the jvm install.

```zsh
sdk install java 25.0.3-tem 
sdk use java 25.0.3-tem
```

Once a JVM is installed you use the gradle target:

```zsh
./gradlew installDist -x test
```

The resulting binary
is `build/install/besu/bin/evmtool`

If the testing repository and besu are installed in the same parent
directory, the command to run the execution tests is

```zsh
fill -v tests --evm-bin ../besu/build/install/besu/bin/evmtool 
```

Assuming homebrew and SDKMan are both installed, the complete script is

```zsh
sdk install java 25.0.3-tem 
sdk use java 25.0.3-tem
git clone https://github.com/hyperledger/besu
cd besu
./gradlew installDist -x test
cd ..

brew install ethereum solidity
solc-select install latest
solc-select use latest
git clone https://github.com/ethereum/execution-spec-tests
cd execution-spec-tests
python3 -m venv ./venv/
source ./venv/bin/activate
pip install -e .\[docs,lint,test\]

fill -v tests --evm-bin ../besu/build/install/besu/bin/evmtool
```

