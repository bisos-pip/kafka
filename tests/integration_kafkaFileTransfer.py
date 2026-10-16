#!/usr/bin/env python3
"""End-to-end test of kafkaFileSend.cs / kafkaFileReceive.cs against a real broker (this host's bisos.kafka).

Run (bisos3 venv; broker up):   python tests/integration_kafkaFileTransfer.py [bootstrap]
Each case uses a fresh topic and group and cleans up. Exit 0 only if every case passed.
"""
import filecmp
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

from bisos.kafka import kafkaFileTransfer as kft

BS = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:24007"
KBIN = "/opt/kafka/current/bin"
RESULTS = []


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def newTopic(tag):
    topic = f"kfx-{tag}-{uuid.uuid4().hex[:8]}"
    r = run([f"{KBIN}/kafka-topics.sh", "--bootstrap-server", BS, "--create", "--topic", topic, "--partitions", "1"])
    assert r.returncode == 0, r.stderr
    return topic


def dropTopic(topic):
    run([f"{KBIN}/kafka-topics.sh", "--bootstrap-server", BS, "--delete", "--topic", topic])


def send(topic, files, headers="run=r1,host=h1", extra=(), cmd="kafkaFileSend.cs"):
    return run([cmd, "-i", "fileSend", f"--bootstrap={BS}", f"--topic={topic}", f"--headers={headers}", *extra, *map(str, files)])


def receive(topic, out, idle=12, extra=(), group=None):
    return run(["kafkaFileReceive.cs", "-i", "fileReceive", f"--bootstrap={BS}", f"--topic={topic}",
                f"--group={group or uuid.uuid4().hex}", f"--outBase={out}", f"--idleExit={idle}", *extra])


def check(name, ok, detail=""):
    RESULTS.append((name, ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""))


def treesEqual(a, b):
    cmp = filecmp.dircmp(a, b)
    def walk(c):
        if c.left_only or c.right_only or c.diff_files or c.funny_files:
            return False
        return all(walk(s) for s in c.subdirs.values())
    same = walk(cmp)
    # dircmp compares by stat signature by default; compare content explicitly too
    for f in pathlib.Path(a).rglob("*"):
        if f.is_file() and (pathlib.Path(b) / f.relative_to(a)).read_bytes() != f.read_bytes():
            return False
    return same


def makeTree(root):
    files = {
        "alpha/p1/siteA/r1/data.txt": b"".join(b"row|%d|alpha|p1\n" % i for i in range(3000)),
        "alpha/p2/siteA/r1/data.txt": b"".join(b"row|%d|alpha|p2\n" % i for i in range(2000)),
        "beta/p2/siteB/r1/data.txt": b"first|1\nsecond|2\nno-final-newline",
        "beta/p1/siteB/r1/one.txt": b"x|1\n",
        "beta/p1/siteB/r1/blank.txt": b"a\n\n\nb\n",
        "alpha/p1/siteA/r1/utf8.txt": "héllo|wörld|✓\n".encode(),
    }
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return sorted(root.rglob("*.txt"))


def main():
    work = pathlib.Path(tempfile.mkdtemp(prefix="kfx-it-"))

    # 1. round trip of a hierarchy, byte for byte --------------------------------------------------------
    t = newTopic("tree")
    src = work / "src"; out = work / "out"
    files = makeTree(src)
    r = send(t, files, extra=[f"--root={src}", "--attrs=vendor=v", "--msgKey=h1"])
    check("send a hierarchy (6 files, one transaction each)", r.returncode == 0, r.stdout[-300:] + r.stderr[-300:])
    r = receive(t, out)
    check("receive exits cleanly", r.returncode == 0, r.stdout[-300:] + r.stderr[-300:])
    check("rebuilt tree is byte-identical, same hierarchy", treesEqual(src, out))

    # 2. duplicate delivery: send again, a new group reads everything again ------------------------------
    send(t, files, extra=[f"--root={src}", "--msgKey=h1"])
    before = {p: p.stat().st_mtime_ns for p in out.rglob("*.txt")}
    r = receive(t, out)
    after = {p: p.stat().st_mtime_ns for p in out.rglob("*.txt")}
    skipped = r.stdout.count("skipped") + r.stderr.count("skipped")
    check("duplicate delivery: every file skipped, none rewritten", before == after and skipped >= 6 and treesEqual(src, out),
          f"skipped={skipped}")

    # 3. a consumer that knows nothing about files: plain rows ---------------------------------------------
    one = src / "alpha/p2/siteA/r1/data.txt"
    r = run([f"{KBIN}/kafka-console-consumer.sh", "--bootstrap-server", BS, "--topic", t, "--from-beginning",
             "--timeout-ms", "10000", "--property", "print.headers=true", "--property", "print.key=true",
             "--isolation-level", "read_committed"])
    rows = [l for l in r.stdout.splitlines() if "alpha|p2" in l]
    plain = rows and all(l.startswith("run:r1,host:h1") or "run:r1,host:h1" in l for l in rows)
    expectLines = one.read_text().splitlines()
    sampleOk = all(any(l.endswith("\t" + e) or l.split("\t")[-1] == e for l in rows) for e in expectLines[:5])
    check("a rows-only consumer sees the raw lines, with the headers", bool(plain) and sampleOk and len(rows) >= 2000,
          f"rows={len(rows)}")
    check("rows-only consumer: file markers exist only as headers (first/last record)",
          sum("kfx-bof" in l for l in r.stdout.splitlines()) >= 6 and not any(l.split("\t")[-1].startswith("{") and "kfx" in l.split("\t")[-1] for l in r.stdout.splitlines()))
    dropTopic(t)

    # 4. records model: plain rows, no markers; the receiver counts them as orphans, writes nothing -------
    t = newTopic("rec"); out2 = work / "out2"
    r = send(t, [one], extra=["--model=records"])
    check("send in records model", r.returncode == 0, r.stdout[-200:] + r.stderr[-200:])
    r = receive(t, out2, idle=8)
    nothing = not out2.exists() or not any(out2.rglob("*"))
    check("receive refuses to rebuild records-model data (orphans, nothing written)",
          nothing and "carried no file markers" in (r.stdout + r.stderr), (r.stdout + r.stderr)[-300:])
    dropTopic(t)

    # 5. concurrent senders into one partition: interleaved transactions ---------------------------------
    t = newTopic("conc"); src3 = work / "src3"; out3 = work / "out3"
    procs = []
    for i in range(8):
        d = src3 / f"s{i}"
        d.mkdir(parents=True)
        f = d / "data.txt"
        f.write_bytes(b"".join(b"s%d|row|%d\n" % (i, n) for n in range(4000)))
        procs.append(subprocess.Popen(["kafkaFileSend.cs", "-i", "fileSend", f"--bootstrap={BS}", f"--topic={t}",
                                       f"--headers=run=r1,src=s{i}", f"--root={src3}", str(f)],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
    codes = [p.wait() for p in procs]
    check("8 concurrent senders all succeed", codes == [0] * 8, str(codes))
    r = receive(t, out3, idle=15)
    check("interleaved files demultiplexed and rebuilt exactly", treesEqual(src3, out3),
          (r.stdout + r.stderr)[-300:])
    dropTopic(t)

    # 6. a file that changes while being sent: aborted, nothing visible, the next file still goes --------
    t = newTopic("chg"); out4 = work / "out4"
    bad = work / "bad.txt"; good = work / "good.txt"
    bad.write_bytes(b"a\nb\nc\n"); good.write_bytes(b"g1\ng2\n")
    res = kft.sendFiles([(bad, "bad.txt"), (good, "good.txt")], BS, t, headers=[("run", "r1")], idHeaders=["run"],
                        say=lambda s: None, _afterDeclare=lambda: bad.write_bytes(b"CHANGED\n") if bad.read_bytes() == b"a\nb\nc\n" else None)
    check("changed file reported failed, the next file sent", [x[1] == "sent" for x in res] == [False, True], str(res))
    r = receive(t, out4, idle=8)
    check("only the good file is visible to a read_committed receiver",
          sorted(p.name for p in out4.rglob("*.txt")) == ["good.txt"], str(list(out4.rglob("*"))))
    dropTopic(t)

    # 7. a sender killed mid-transaction: nothing visible; readers resume after the transaction timeout ----
    t = newTopic("kill"); out5 = work / "out5"
    killer = (
        "import os,sys; from confluent_kafka import Producer\n"
        f"p=Producer({{'bootstrap.servers':'{BS}','transactional.id':'kill-{uuid.uuid4().hex[:6]}','transaction.timeout.ms':10000}})\n"
        "p.init_transactions(20); p.begin_transaction()\n"
        f"[p.produce('{t}', value=b'half', headers=[('run','rk')]) for _ in range(500)]\n"
        "p.flush(); os._exit(1)\n")
    subprocess.run([sys.executable, "-c", killer])
    g = work / "after.txt"; g.write_bytes(b"after|1\n")
    t0 = time.time()
    send(t, [g], headers="run=r2")
    r = receive(t, out5, idle=25)
    waited = time.time() - t0
    check("killed sender: its records never visible; the later good file arrives",
          sorted(p.name for p in out5.rglob("*.txt")) == ["after.txt"] and not r.stdout.count("half"),
          f"{waited:.0f}s; {(r.stdout + r.stderr)[-200:]}")
    dropTopic(t)

    # 8. a marker carrying a hostile path is rejected -- and the good file behind it still arrives ----------------
    t = newTopic("evil"); out6 = work / "out6"; outside = work / "pwned.txt"
    from confluent_kafka import Producer
    import hashlib

    def sendRaw(path, data, run):
        bof = {"v": 1, "path": path, "attrs": {}, "bytes": len(data), "rows": 1, "sha256": hashlib.sha256(data).hexdigest(),
               "eol": "\n", "finalEol": True, "idHeaders": ["run"], "hdr": {"mode": "kafka"}}
        eof = {k: bof[k] for k in ("v", "path", "bytes", "rows", "sha256")}
        p = Producer({"bootstrap.servers": BS, "transactional.id": "raw-" + uuid.uuid4().hex[:6]})
        p.init_transactions(20); p.begin_transaction()
        p.produce(t, value=data.rstrip(b"\n"), headers=[("run", run), ("kfx-bof", json.dumps(bof).encode()),
                                                        ("kfx-eof", json.dumps(eof).encode())])
        p.commit_transaction(20)

    sendRaw("../pwned.txt", b"evil\n", "x")
    sendRaw("good/ok.txt", b"fine\n", "y")
    r = receive(t, out6, idle=8)
    text = r.stdout + r.stderr
    check("hostile ../ path: rejected, reported, nothing written outside --outBase",
          not outside.exists() and "REJECTED" in text, text[-300:])
    check("the good file behind the hostile one still arrives (no poison pill)",
          (out6 / "good/ok.txt").read_bytes() == b"fine\n")
    check("a receive that rejected something exits non-zero", r.returncode != 0, f"rc={r.returncode}")
    dropTopic(t)

    # 9. exit status: failures are not silent --------------------------------------------------------------------
    r = send("kfx-none", [work / "does-not-exist.txt"])
    check("send of a missing file exits non-zero", r.returncode != 0, f"rc={r.returncode}")

    # 10. the "Try it" example: the demo file, sent with no --root, rebuilt under --outBase by its name ---------
    from bisos.kafka import kafkaFileTransfer_csu as csu
    t = newTopic("demo"); out7 = work / "out7"
    r = send(t, [csu.demoFile], headers="host=h1")
    check("Try it: send of the demo file", r.returncode == 0, (r.stdout + r.stderr)[-200:])
    r = receive(t, out7, idle=int(csu.demoIdleExit))
    check("Try it: rebuilt byte for byte under --outBase by its name",
          (got := out7 / csu.demoFile.name).is_file() and got.read_bytes() == csu.demoFile.read_bytes(),
          (r.stdout + r.stderr)[-200:])
    dropTopic(t)

    failed = [n for n, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
