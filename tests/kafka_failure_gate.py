"""Run userver failure/commit checks on a disposable Kafka; no shared restarts."""
import argparse
from pathlib import Path
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--artifacts', required=True)
    args = parser.parse_args()
    root = Path(args.artifacts).resolve()
    root.mkdir(parents=True, exist_ok=True)
    sdk = Path(__file__).resolve().parents[1]
    broker = 'sa-userver-failure-' + uuid.uuid4().hex
    settings = {
        'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_LISTENERS': 'BROKER://:9092,CONTROLLER://:9093',
        'KAFKA_ADVERTISED_LISTENERS': 'BROKER://127.0.0.1:9092',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'BROKER',
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@127.0.0.1:9093',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
        'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
    }
    command = ['docker', 'run', '-d', '--name', broker]
    for key, value in settings.items():
        command += ['-e', key + '=' + value]
    try:
        subprocess.run(command + ['apache/kafka:4.1.2'], check=True, capture_output=True)
        command = [
            'docker', 'run', '--rm', '--network', 'container:' + broker,
            '-v', str(sdk) + ':/workspace', '-v', 'cppservicelib_cppservicelib-conan2:/conan:ro',
            '-e', 'SA_TEST_KAFKA_BROKERS=127.0.0.1:9092', '--entrypoint', 'bash',
            'cppservicelib-build:latest', '-lc',
            'cmake --build /workspace/build-nats/debug --target servicelib_custom_kafka_endpoints_test -j2 && '
            '/workspace/build-nats/debug/servicelib_custom_kafka_endpoints_test '
            '--gtest_filter=KafkaNativeTransactions.FailedPartitionCannotBeCoveredByBatchCommit:KafkaDataSource.*:KafkaCheckpoint.*',
        ]
        with (root / 'acceptance.log').open('w') as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=240)
        if result.returncode:
            raise RuntimeError(f'Kafka failure gate failed: {root}/acceptance.log')
        print(f'PASS userver Kafka failure gate: {root}')
    finally:
        log = subprocess.run(['docker', 'logs', broker], capture_output=True, text=True)
        (root / 'broker.log').write_text(log.stdout + log.stderr)
        subprocess.run(['docker', 'rm', '-f', broker], capture_output=True)


if __name__ == '__main__':
    main()
