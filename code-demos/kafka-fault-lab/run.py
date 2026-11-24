#!/usr/bin/env python3
"""真实 Kafka 三节点故障实验；只管理本次创建的 Docker 资源。"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import uuid

import confluent_kafka as kafka
from confluent_kafka.admin import AdminClient, NewTopic


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def docker(*args):
    result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f'docker {args[0]} failed: {result.stderr}')
    return result.stdout.strip()


def eventually(fn, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (kafka.KafkaException, KeyError) as error:
            last = str(error)
        time.sleep(0.5)
    raise TimeoutError(f'Condition not met after {timeout}s; last error: {last}')


@contextmanager
def cluster(image):
    prefix = 'interview-kafka-' + uuid.uuid4().hex[:10]
    network = prefix + '-net'
    names = {i: f'{prefix}-{i}' for i in range(1, 4)}
    created = []
    ports = []
    # Docker 最终负责独占端口；极小竞争窗口内被占用时直接失败并清理。
    for _ in range(3):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            ports.append(sock.getsockname()[1])
    docker('network', 'create', '--label', 'purpose=interview-kafka-lab', network)
    try:
        for i, name in names.items():
            env = {
                'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',
                'KAFKA_NODE_ID': str(i), 'KAFKA_PROCESS_ROLES': 'broker,controller',
                'KAFKA_CONTROLLER_QUORUM_VOTERS': ','.join(f'{j}@{n}:9093' for j, n in names.items()),
                'KAFKA_LISTENERS': 'INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094',
                'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://{name}:9092,EXTERNAL://127.0.0.1:{ports[i-1]}',
                'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT',
                'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
                'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '3',
                'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '3',
                'KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS': '3',
                'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '3',
                'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '2',
                'KAFKA_MIN_INSYNC_REPLICAS': '2',
                'KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE': 'false',
                'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
                'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
                'KAFKA_REPLICA_LAG_TIME_MAX_MS': '5000',
                'KAFKA_HEAP_OPTS': '-Xms256m -Xmx256m',
            }
            args = ['create', '--name', name, '--network', network,
                    '--label', 'purpose=interview-kafka-lab', '--memory', '768m',
                    '--publish', f'127.0.0.1:{ports[i-1]}:9094']
            for key, value in env.items():
                args.extend(['--env', f'{key}={value}'])
            created.append(docker(*args, image))
            docker('start', name)
        bootstrap = ','.join(f'127.0.0.1:{p}' for p in ports)
        admin = AdminClient({'bootstrap.servers': bootstrap, 'log_level': 0})
        eventually(lambda: len(admin.list_topics(timeout=3).brokers) == 3, 120)
        yield dict(names=names, network=network, bootstrap=bootstrap, admin=admin,
                   image_id=docker('inspect', '--format', '{{.Image}}', names[1]))
    except Exception:
        for container in created:
            print(docker('logs', '--tail', '12', container), flush=True)
        raise
    finally:
        # 清理尽量覆盖所有已创建资源，不操作任何其他项目的容器。
        errors = []
        for container in reversed(created):
            try:
                docker('rm', '--force', '--volumes', container)
            except RuntimeError as error:
                errors.append(str(error))
        try:
            docker('network', 'rm', network)
        except RuntimeError as error:
            errors.append(str(error))
        if errors:
            raise RuntimeError('Cleanup failed: ' + '; '.join(errors))


def producer(bootstrap, **extra):
    return kafka.Producer({'bootstrap.servers': bootstrap, 'acks': 'all',
                           'enable.idempotence': True, 'message.timeout.ms': 30000,
                           'request.timeout.ms': 3000, 'socket.timeout.ms': 5000,
                           'socket.connection.setup.timeout.ms': 5000,
                           'topic.metadata.refresh.interval.ms': 1000, 'log_level': 0, **extra})


def send(p, topic, ids, allow_failure=False):
    outcomes = []
    for event_id in ids:
        value = json.dumps({'id': event_id, 'amount': 10})
        p.produce(topic, key=str(event_id), value=value,
                  on_delivery=lambda error, message: outcomes.append(
                      {'error': error.name() if error else None,
                       'offset': message.offset()}))
    check(p.flush(40) == 0, 'Producer did not drain within deadline')
    check(len(outcomes) == len(ids), 'Missing delivery callbacks')
    if not allow_failure:
        check(all(x['error'] is None for x in outcomes), str(outcomes))
    return outcomes


def state(c, topic):
    partition = c['admin'].list_topics(topic, timeout=4).topics[topic].partitions[0]
    return dict(leader=partition.leader, isr=sorted(partition.isrs), replicas=sorted(partition.replicas))


def ready(c, topic, size=3, excluded=None):
    def inspect():
        s = state(c, topic)
        return s if len(s['isr']) == size and s['leader'] > 0 and s['leader'] != excluded else None
    return eventually(inspect)


def new_topic(c, name):
    future = c['admin'].create_topics([NewTopic(name, num_partitions=1, replication_factor=3,
        config={'min.insync.replicas': '2', 'unclean.leader.election.enable': 'false'})])[name]
    future.result(timeout=30)
    return ready(c, name)


def consumer(bootstrap, group, isolation='read_committed'):
    return kafka.Consumer({'bootstrap.servers': bootstrap, 'group.id': group,
                           'enable.auto.commit': False, 'enable.auto.offset.store': False,
                           'auto.offset.reset': 'earliest', 'enable.partition.eof': True,
                           'isolation.level': isolation, 'log_level': 0})


def read_all(bootstrap, topic, isolation='read_committed'):
    c = consumer(bootstrap, 'audit-' + uuid.uuid4().hex, isolation)
    c.assign([kafka.TopicPartition(topic, 0, kafka.OFFSET_BEGINNING)])
    values = []
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            message = c.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == kafka.KafkaError._PARTITION_EOF:
                    return values
                raise kafka.KafkaException(message.error())
            values.append(json.loads(message.value()))
        raise TimeoutError('Consumer did not reach partition EOF')
    finally:
        c.close()


def audit(bootstrap, topic, expected, allow_duplicates=False):
    values = read_all(bootstrap, topic)
    check(set(x['id'] for x in values) == set(expected), 'Missing or unexpected event IDs')
    check(all(x['amount'] == 10 for x in values), 'Unexpected payload')
    if not allow_duplicates:
        check(len(values) == len(expected), 'Unexpected duplicate records')
    return dict(records=len(values), unique_ids=len(expected),
                duplicate_records=len(values)-len(expected), missing_ids=[], unexpected_ids=[],
                unique_amount=sum({x['id']: x['amount'] for x in values}.values()))


def network_evidence(c, node):
    info = json.loads(docker('inspect', c['names'][node]))[0]
    check(info['State']['Running'], 'Partitioned broker must remain running')
    check(c['network'] not in info['NetworkSettings']['Networks'], 'Network still attached')
    return dict(node=node, process_running=True, experiment_network_attached=False)


def recoverable_fault(c, mode):
    topic = 'lab-' + mode
    before = new_topic(c, topic)
    p = producer(c['bootstrap'])
    send(p, topic, list(range(20)))
    before = ready(c, topic)
    leader = before['leader']
    started = time.monotonic()
    if mode == 'leader_kill':
        docker('kill', '--signal', 'KILL', c['names'][leader])
        info = json.loads(docker('inspect', c['names'][leader]))[0]['State']
        check(not info['Running'] and info['ExitCode'] == 137, 'Broker was not killed')
        evidence = dict(exit_code=info['ExitCode'], oom_killed=info['OOMKilled'])
    else:
        docker('network', 'disconnect', c['network'], c['names'][leader])
        evidence = network_evidence(c, leader)
    during = ready(c, topic, size=2, excluded=leader)
    election_seconds = round(time.monotonic() - started, 3)
    send(p, topic, list(range(20, 40)))
    if mode == 'leader_kill':
        docker('start', c['names'][leader])
    else:
        docker('network', 'connect', c['network'], c['names'][leader])
    restored = ready(c, topic)
    return dict(before=before, fault=evidence, during=during, restored=restored,
                observed_failover_seconds=election_seconds,
                audit=audit(c['bootstrap'], topic, range(40)))


def quorum_loss(c):
    topic = 'lab-quorum-loss'
    before = new_topic(c, topic)
    p = producer(c['bootstrap'], **{'message.timeout.ms': 12000})
    send(p, topic, list(range(20)))
    before = ready(c, topic)
    # 隔离两个 follower，同时失去 controller 多数派。不能单独归因为 min ISR。
    nodes = [i for i in c['names'] if i != before['leader']]
    evidence = []
    for node in nodes:
        docker('network', 'disconnect', c['network'], c['names'][node])
        evidence.append(network_evidence(c, node))
    outcomes = send(p, topic, [20], allow_failure=True)
    check(outcomes[0]['error'] is not None, 'Write unexpectedly acknowledged without quorum')
    for node in nodes:
        docker('network', 'connect', c['network'], c['names'][node])
    restored = ready(c, topic)
    # 超时结果未知；使用同一业务 ID 重发，允许 Kafka 中存在重复 ID。
    send(producer(c['bootstrap']), topic, list(range(20, 40)))
    return dict(before=before, fault=evidence, failed_delivery=outcomes,
                restored=restored, audit=audit(c['bootstrap'], topic, range(40), True))


def sink_init(path):
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE processed(id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)')
        db.execute('CREATE TABLE balance(amount INTEGER NOT NULL)')
        db.execute('INSERT INTO balance VALUES(0)')


def sink_apply(path, value):
    with sqlite3.connect(path) as db:
        inserted = db.execute('INSERT OR IGNORE INTO processed VALUES (?,?)',
                              (value['id'], value['amount'])).rowcount
        if inserted:
            db.execute('UPDATE balance SET amount=amount+?', (value['amount'],))
    return inserted


def sink_state(path):
    with sqlite3.connect(path) as db:
        return dict(ids=[r[0] for r in db.execute('SELECT id FROM processed ORDER BY id')],
                    amount=db.execute('SELECT amount FROM balance').fetchone()[0])


def consume_to_sink(bootstrap, topic, group, path, pipe=None):
    c = consumer(bootstrap, group)
    partition = kafka.TopicPartition(topic, 0)
    try:
        offset = c.committed([partition], timeout=15)[0].offset
        c.assign([kafka.TopicPartition(topic, 0, offset if offset >= 0 else kafka.OFFSET_BEGINNING)])
        attempts = duplicates = 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            message = c.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == kafka.KafkaError._PARTITION_EOF:
                    return dict(attempts=attempts, duplicates=duplicates,
                                committed=c.committed([partition], timeout=10)[0].offset)
                raise kafka.KafkaException(message.error())
            value = json.loads(message.value())
            inserted = sink_apply(path, value)
            attempts += 1
            duplicates += 1-inserted
            if pipe is not None:
                pipe.send(dict(event_id=value['id'], offset=message.offset(), database_committed=True))
                pipe.recv()  # 父进程在 DB 提交后、Kafka 位点提交前 SIGKILL。
            c.commit(message=message, asynchronous=False)
        raise TimeoutError('Sink did not reach EOF')
    finally:
        c.close()


def consumer_crash(c, directory):
    topic, group = 'lab-consumer-crash', 'sink-crash-group'
    new_topic(c, topic)
    send(producer(c['bootstrap']), topic, list(range(10)))
    path = str(Path(directory) / 'sink.sqlite')
    sink_init(path)
    ctx = multiprocessing.get_context('spawn')
    parent, child = ctx.Pipe()
    worker = ctx.Process(target=consume_to_sink, args=(c['bootstrap'], topic, group, path, child))
    worker.start()
    child.close()
    try:
        check(parent.poll(40), 'Consumer did not reach DB commit barrier')
        barrier = parent.recv()
        worker.kill()
        worker.join(5)
        check(worker.exitcode == -signal.SIGKILL, 'Worker was not killed')
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join(5)
        parent.close()
        worker.close()
    before = sink_state(path)
    check(before == {'ids': [0], 'amount': 10}, str(before))
    probe = consumer(c['bootstrap'], group)
    try:
        committed_before = probe.committed([kafka.TopicPartition(topic, 0)], timeout=15)[0].offset
        check(committed_before < 0, 'Offset was committed before crash')
    finally:
        probe.close()
    resumed = consume_to_sink(c['bootstrap'], topic, group, path)
    after = sink_state(path)
    check(after == {'ids': list(range(10)), 'amount': 100}, str(after))
    check(resumed == {'attempts': 10, 'duplicates': 1, 'committed': 10}, str(resumed))
    return dict(barrier=barrier, signal='SIGKILL', before=before,
                committed_before=committed_before, resumed=resumed, after=after)


def transactions(c):
    topic = 'lab-transactions'
    new_topic(c, topic)
    p = producer(c['bootstrap'], **{'transactional.id': 'lab-transaction-producer'})
    p.init_transactions(30)
    p.begin_transaction()
    send(p, topic, list(range(5)))
    p.abort_transaction(30)
    p.begin_transaction()
    send(p, topic, list(range(5, 10)))
    p.commit_transaction(30)
    committed = audit(c['bootstrap'], topic, range(5, 10))
    uncommitted = read_all(c['bootstrap'], topic, 'read_uncommitted')
    check([x['id'] for x in uncommitted] == list(range(10)), 'Raw transaction records differ')
    return dict(read_committed=committed, read_uncommitted_records=len(uncommitted))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', default='apache/kafka:4.1.1')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    results = []
    with cluster(args.image) as c, tempfile.TemporaryDirectory(prefix='kafka-sink-') as directory:
        metadata = dict(image=args.image, image_id=c['image_id'],
                        broker_version=docker('exec', c['names'][1], '/opt/kafka/bin/kafka-topics.sh', '--version'),
                        client=kafka.__version__, librdkafka=kafka.libversion()[0],
                        sqlite=sqlite3.sqlite_version, brokers=3, replication_factor=3,
                        min_insync_replicas=2, acks='all', idempotence=True,
                        roles='combined broker/controller; static KRaft quorum')
        for name, fn in [
            ('leader_kill', lambda: recoverable_fault(c, 'leader_kill')),
            ('leader_network_partition', lambda: recoverable_fault(c, 'leader_network_partition')),
            ('quorum_network_partition', lambda: quorum_loss(c)),
            ('consumer_commit_gap', lambda: consumer_crash(c, directory)),
            ('transaction_visibility', lambda: transactions(c)),
        ]:
            results.append(dict(name=name, observed=fn()))
            print('PASS', name, json.dumps(results[-1]['observed']), flush=True)
    report = dict(completed_at=datetime.now(timezone.utc).isoformat(), environment=metadata,
                  cleanup='created containers, anonymous volumes, network and sink files removed', cases=results)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(f'{len(results)} experiments passed; report: {args.report}', flush=True)


if __name__ == '__main__':
    main()
