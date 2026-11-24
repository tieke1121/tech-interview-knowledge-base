#!/usr/bin/env python3
"""在独立 PostgreSQL 容器中验证并发与应用进程崩溃后的业务不变量。"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import multiprocessing
import os
from pathlib import Path
import secrets
import signal
import subprocess
import threading
import time
import uuid

import psycopg


def check(actual, expected):
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, capture_output=True,
                          text=True, timeout=120, **kwargs).stdout.strip()


def connect(config):
    return psycopg.connect(**config, autocommit=True, connect_timeout=3,
                          options="-c statement_timeout=10000 -c lock_timeout=7000")


@contextmanager
def database(image):
    name = "interview-correctness-" + uuid.uuid4().hex[:12]
    environment = dict(os.environ, POSTGRES_PASSWORD=secrets.token_urlsafe(24))
    container_id = docker("run", "--detach", "--name", name, "--label",
                          "purpose=interview-correctness-lab", "--memory", "512m",
                          "--publish", "127.0.0.1::5432", "--env", "POSTGRES_PASSWORD",
                          "--env", "POSTGRES_DB=interview_lab", image, env=environment)
    try:
        address = docker("port", container_id, "5432/tcp")
        config = dict(host="127.0.0.1", port=int(address.rsplit(":", 1)[1]),
                      dbname="interview_lab", user="postgres",
                      password=environment["POSTGRES_PASSWORD"])
        deadline = time.monotonic() + 60
        while True:
            try:
                with connect(config) as connection:
                    version = connection.execute("SELECT version()").fetchone()[0]
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Temporary PostgreSQL did not become ready") from None
                time.sleep(0.3)
        metadata = dict(image=image, image_id=docker("inspect", "--format", "{{.Image}}", container_id),
                        database=version, driver=psycopg.__version__)
        yield config, metadata
    finally:
        # 仅清理由本次运行创建的容器及其匿名卷。
        docker("rm", "--force", "--volumes", container_id)


def setup(config):
    with connect(config) as db:
        db.execute("""
            CREATE TABLE stock (id integer PRIMARY KEY, quantity integer NOT NULL);
            INSERT INTO stock VALUES (1, 1), (2, 1);
            CREATE TABLE orders (request_id text PRIMARY KEY, amount integer NOT NULL);
            CREATE TABLE outbox (event_id text PRIMARY KEY, amount integer NOT NULL, sent boolean DEFAULT false);
            CREATE TABLE deliveries (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                event_id text NOT NULL, amount integer NOT NULL, acked boolean DEFAULT false);
            CREATE TABLE processed (event_id text PRIMARY KEY);
            CREATE TABLE account (id integer PRIMARY KEY, amount integer NOT NULL);
            INSERT INTO account VALUES (1, 0);
            CREATE TABLE entity (id integer PRIMARY KEY, version integer, amount integer, deleted boolean);
            CREATE SEQUENCE fence_tokens;
            CREATE TABLE resource (id integer PRIMARY KEY, token bigint, value text);
            INSERT INTO resource VALUES (1, 0, 'initial');
        """)


def wait_for_lock(observer, backend_pid):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        row = observer.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                               (backend_pid,)).fetchone()
        if row and row[0] == "Lock":
            return
        time.sleep(0.02)
    raise AssertionError("Second connection never entered a database lock wait")


def stock_race(config, rollback):
    with connect(config) as a, connect(config) as b, connect(config) as observer:
        a.execute("UPDATE stock SET quantity=1 WHERE id=1")
        a.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        check(a.execute("UPDATE stock SET quantity=quantity-1 WHERE id=1 AND quantity>=1").rowcount, 1)
        b.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(b.execute,
                                 "UPDATE stock SET quantity=quantity-1 WHERE id=1 AND quantity>=1")
            try:
                wait_for_lock(observer, b.info.backend_pid)
            finally:
                # 即使观察失败，也释放 A，确保线程可以退出。
                if rollback:
                    a.rollback()
                else:
                    a.commit()
            affected = future.result(timeout=10).rowcount
            b.commit()
        check(affected, int(rollback))
        quantity = observer.execute("SELECT quantity FROM stock WHERE id=1").fetchone()[0]
        check(quantity, 0)
        return dict(lock_wait_observed=True, second_affected=affected, final_quantity=quantity)


def isolation(config, repeatable):
    with connect(config) as a, connect(config) as b:
        b.execute("UPDATE stock SET quantity=1 WHERE id=1")
        level = "REPEATABLE READ" if repeatable else "READ COMMITTED"
        a.execute("BEGIN ISOLATION LEVEL " + level)
        first = a.execute("SELECT quantity FROM stock WHERE id=1").fetchone()[0]
        b.execute("UPDATE stock SET quantity=2 WHERE id=1")
        second = a.execute("SELECT quantity FROM stock WHERE id=1").fetchone()[0]
        a.commit()
        check([first, second], [1, 1 if repeatable else 2])
        return dict(isolation=level, observed=[first, second])


def deadlock(config):
    with connect(config) as db:
        db.execute("UPDATE stock SET quantity=1")
    barrier = threading.Barrier(2, timeout=5)

    def update_pair(first, second):
        try:
            with connect(config) as db, db.transaction():
                db.execute("UPDATE stock SET quantity=quantity+1 WHERE id=%s", (first,))
                barrier.wait()
                db.execute("UPDATE stock SET quantity=quantity+1 WHERE id=%s", (second,))
            return "committed"
        except psycopg.errors.DeadlockDetected as error:
            return error.sqlstate

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(update_pair, 1, 2), pool.submit(update_pair, 2, 1)]
        outcomes = sorted(f.result(timeout=15) for f in futures)
    check(outcomes, ["40P01", "committed"])
    with connect(config) as db:
        values = db.execute("SELECT quantity FROM stock ORDER BY id").fetchall()
    check(values, [(2,), (2,)])
    return dict(outcomes=outcomes, quantities=[row[0] for row in values])


def checkpoint(target, current, pipe):
    if target == current:
        pipe.send(current)
        pipe.recv()  # 父进程收到屏障后发送 SIGKILL，子进程不会正常越过这里。


def produce(config, target=None, pipe=None):
    with connect(config) as db, db.transaction():
        created = db.execute("INSERT INTO orders VALUES ('request-1',10) ON CONFLICT DO NOTHING RETURNING request_id").fetchone()
        if created:
            db.execute("INSERT INTO outbox(event_id,amount) VALUES ('event-1',10)")
        checkpoint(target, "before_producer_commit", pipe)
    checkpoint(target, "after_producer_commit", pipe)


def relay(config, target=None, pipe=None):
    with connect(config) as producer, connect(config) as transport:
        events = producer.execute("SELECT event_id,amount FROM outbox WHERE NOT sent").fetchall()
        for event_id, amount in events:
            # 独立连接的 autocommit 模拟消息系统已经持久接收；允许重复投递。
            transport.execute("INSERT INTO deliveries(event_id,amount) VALUES (%s,%s)", (event_id, amount))
            checkpoint(target, "after_publish", pipe)
            producer.execute("UPDATE outbox SET sent=true WHERE event_id=%s", (event_id,))


def consume(config, target=None, pipe=None):
    with connect(config) as transport, connect(config) as consumer:
        deliveries = transport.execute("SELECT id,event_id,amount FROM deliveries WHERE NOT acked ORDER BY id").fetchall()
        for delivery_id, event_id, amount in deliveries:
            with consumer.transaction():
                inserted = consumer.execute("INSERT INTO processed VALUES (%s) ON CONFLICT DO NOTHING RETURNING event_id",
                                            (event_id,)).fetchone()
                if inserted:
                    consumer.execute("UPDATE account SET amount=amount+%s WHERE id=1", (amount,))
                checkpoint(target, "before_consumer_commit", pipe)
            checkpoint(target, "after_consumer_commit", pipe)
            transport.execute("UPDATE deliveries SET acked=true WHERE id=%s", (delivery_id,))


def workflow(config, target=None, pipe=None):
    produce(config, target, pipe)
    relay(config, target, pipe)
    consume(config, target, pipe)


def reset_delivery(config):
    with connect(config) as db:
        db.execute("TRUNCATE orders,outbox,deliveries,processed RESTART IDENTITY")
        db.execute("UPDATE account SET amount=0")


def delivery_state(config):
    with connect(config) as db:
        return list(db.execute("""SELECT
            (SELECT count(*) FROM orders), (SELECT count(*) FROM outbox),
            (SELECT count(*) FROM deliveries), (SELECT count(*) FROM processed),
            (SELECT amount FROM account WHERE id=1),
            (SELECT count(*) FROM deliveries WHERE NOT acked)""").fetchone())


def crash_recovery(config, stage, expected_before):
    reset_delivery(config)
    ctx = multiprocessing.get_context("spawn")
    parent, child = ctx.Pipe()
    process = ctx.Process(target=workflow, args=(config, stage, child))
    process.start()
    child.close()
    try:
        if not parent.poll(15):
            raise TimeoutError("Worker failed to reach " + stage)
        check(parent.recv(), stage)
        process.kill()
        process.join(timeout=5)
        check(process.exitcode, -signal.SIGKILL)
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        parent.close()
        process.close()
    before = delivery_state(config)
    check(before, expected_before)
    # 从同一业务请求恢复两次，再主动重复消费所有投递，检查业务去重。
    workflow(config)
    workflow(config)
    with connect(config) as db:
        db.execute("UPDATE deliveries SET acked=false")
    consume(config)
    after = delivery_state(config)
    expected_deliveries = 2 if stage == "after_publish" else 1
    check(after, [1, 1, expected_deliveries, 1, 10, 0])
    return dict(signal="SIGKILL", before=before, recovered=after)


def concurrent_idempotency(config):
    reset_delivery(config)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(produce, config) for _ in range(6)]
        for future in futures:
            future.result(timeout=15)
    relay(config)
    consume(config)
    state = delivery_state(config)
    check(state, [1, 1, 1, 1, 10, 0])
    return dict(requests=6, final=state)


def versions(config):
    with connect(config) as db:
        changes = [(1, 100, False), (2, 80, False), (1, 100, False),
                   (3, 0, True), (2, 80, False)]
        affected = []
        for version, amount, deleted in changes:
            affected.append(db.execute("""INSERT INTO entity VALUES (1,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET version=excluded.version,
                    amount=excluded.amount, deleted=excluded.deleted
                WHERE entity.version < excluded.version""", (version, amount, deleted)).rowcount)
        row = db.execute("SELECT version,amount,deleted FROM entity WHERE id=1").fetchone()
    check(affected, [1, 1, 0, 1, 0])
    check(row, (3, 0, True))
    return dict(affected=affected, final=list(row))


def fencing(config):
    with connect(config) as old, connect(config) as new:
        old_token = old.execute("SELECT nextval('fence_tokens')").fetchone()[0]
        new_token = new.execute("SELECT nextval('fence_tokens')").fetchone()[0]
        sql = "UPDATE resource SET token=%s,value=%s WHERE id=1 AND token<%s"
        check(new.execute(sql, (new_token, "new", new_token)).rowcount, 1)
        stale_affected = old.execute(sql, (old_token, "old", old_token)).rowcount
        check(stale_affected, 0)
        check(old.execute("SELECT value FROM resource").fetchone()[0], "new")
        # 负例：资源端省略 token 校验后，旧写确实覆盖了新值。
        old.execute("UPDATE resource SET value='old' WHERE id=1")
        unguarded = old.execute("SELECT value FROM resource").fetchone()[0]
        check(unguarded, "old")
    return dict(old_token=old_token, new_token=new_token, stale_affected=stale_affected,
                guarded_value="new", unguarded_value=unguarded)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="postgres:17-alpine")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    results = []
    with database(args.image) as (config, metadata):
        setup(config)
        cases = [
            ("stock_commit", lambda: stock_race(config, False)),
            ("stock_rollback", lambda: stock_race(config, True)),
            ("read_committed", lambda: isolation(config, False)),
            ("repeatable_read", lambda: isolation(config, True)),
            ("deadlock", lambda: deadlock(config)),
        ]
        for name, experiment in cases:
            results.append(dict(name=name, observed=experiment()))
            print("PASS", name, flush=True)
        faults = {
            "before_producer_commit": [0, 0, 0, 0, 0, 0],
            "after_producer_commit": [1, 1, 0, 0, 0, 0],
            "after_publish": [1, 1, 1, 0, 0, 1],
            "before_consumer_commit": [1, 1, 1, 0, 0, 1],
            "after_consumer_commit": [1, 1, 1, 1, 10, 1],
        }
        for name, before in faults.items():
            results.append(dict(name=name, observed=crash_recovery(config, name, before)))
            print("PASS", name, flush=True)
        for name, experiment in [("concurrent_idempotency", concurrent_idempotency),
                                 ("version_and_tombstone", versions), ("fencing", fencing)]:
            results.append(dict(name=name, observed=experiment(config)))
            print("PASS", name, flush=True)
    report = dict(completed_at=datetime.now(timezone.utc).isoformat(), environment=metadata,
                  transport="PostgreSQL durable delivery table, independent transactions; no Kafka broker",
                  cleanup="temporary container and anonymous volumes removed", cases=results)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{len(results)} experiments passed; report: {args.report}")


if __name__ == "__main__":
    main()
