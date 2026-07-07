import requests
import argparse


def create_artifact_paths(version):
    artifacts_base_path = "https://hyperledger.jfrog.io/hyperledger/besu-maven/org/hyperledger/besu"
    # add to this list here to update the list of artifacts to check 
    artifacts = [
        # besu-evm
        f"{artifacts_base_path}/besu-evm/{version}/besu-evm-{version}.module",
        f"{artifacts_base_path}/besu-evm/{version}/besu-evm-{version}.pom",
        f"{artifacts_base_path}/besu-evm/{version}/besu-evm-{version}.jar",
        # besu-plugin-api
        f"{artifacts_base_path}/besu-plugin-api/{version}/besu-plugin-api-{version}.module",
        f"{artifacts_base_path}/besu-plugin-api/{version}/besu-plugin-api-{version}.pom",
        f"{artifacts_base_path}/besu-plugin-api/{version}/besu-plugin-api-{version}.jar",
        # besu-metrics-core
        f"{artifacts_base_path}/internal/besu-metrics-core/{version}/besu-metrics-core-{version}.module",
        f"{artifacts_base_path}/internal/besu-metrics-core/{version}/besu-metrics-core-{version}.pom",
        f"{artifacts_base_path}/internal/besu-metrics-core/{version}/besu-metrics-core-{version}.jar",
        # internal/besu-ethereum-core
        f"{artifacts_base_path}/internal/besu-ethereum-core/{version}/besu-ethereum-core-{version}.module",
        f"{artifacts_base_path}/internal/besu-ethereum-core/{version}/besu-ethereum-core-{version}.pom",
        f"{artifacts_base_path}/internal/besu-ethereum-core/{version}/besu-ethereum-core-{version}.jar",
        # internal/besu-config
        f"{artifacts_base_path}/internal/besu-config/{version}/besu-config-{version}.module",
        f"{artifacts_base_path}/internal/besu-config/{version}/besu-config-{version}.pom",
        f"{artifacts_base_path}/internal/besu-config/{version}/besu-config-{version}.jar",
        # bom
        f"{artifacts_base_path}/bom/{version}/bom-{version}.module",
        f"{artifacts_base_path}/bom/{version}/bom-{version}.pom",
    ]
    return artifacts



def check_url(url):
    print(f"Checking artifact at: {url}")
    r = requests.head(url)
    if (r.status_code != 200):
        raise Exception(f"Sorry, No artifact found at '{url}' !!!")

def main():
    parser = argparse.ArgumentParser(description='Check besu artifacts')
    parser.add_argument('--besu_version', action="store", dest='besu_version', default="")
    args = parser.parse_args()
    print(args.besu_version)

    artifacts = create_artifact_paths(args.besu_version)
    print(artifacts)
    for url in artifacts:
        check_url(url)

if __name__ == "__main__":
    main()