# -*- coding: utf-8 -*-
"""Files over Kafka: send a file as headed records, optionally rebuild it on the other side.

A pure module: the only third-party import is the Kafka client (confluent-kafka), imported
inside the functions that talk to a broker, so everything else is testable without one.
The design, its limits and its reasons are in AI-WorkPlan.org, Stage 2, "The concept".

Records mode (the base): each line of a file is one record. Value = the line without its
terminator; key and headers come from parameters; one transaction per file.

Files mode (optional, on top): the first record also carries header kfx-bof (JSON, what the
sender declared) and the last carries kfx-eof (JSON, what the sender observed). A receiver can
then rebuild the file exactly. A consumer that ignores files sees plain, headed rows.

Nothing here knows any particular consumer: topic, key, header names, identity headers and the
output path are all parameters.
"""

import dataclasses
import hashlib
import json
import os
import pathlib
import socket
import time
import typing

FORMAT_VERSION = 1
HDR_BOF = "kfx-bof"
HDR_EOF = "kfx-eof"

EOL_NAMES = {"lf": "\n", "crlf": "\r\n", "cr": "\r"}


class FileTransferError(Exception):
    """Base of every error this module raises on purpose."""


class FileChangedError(FileTransferError):
    """Sender: the file differs from what was declared. The transaction must be aborted."""


class VerifyError(FileTransferError):
    """Receiver: a rebuilt file does not match its markers. It must not be written."""


class UnsafePathError(FileTransferError):
    """A path from a marker or a template would leave the output directory, or is not relative."""


# --------------------------------------------------------------------------------------------
# Small parsers for command-line values
# --------------------------------------------------------------------------------------------

def parseKeyValues(spec: typing.Optional[str]) -> typing.List[typing.Tuple[str, str]]:
    """'a=1,b=2' -> [('a','1'),('b','2')]. Order is kept; a value may contain '='."""
    if not spec:
        return []
    pairs = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise FileTransferError(f"expected name=value, got {item!r}")
        name, value = item.split("=", 1)
        pairs.append((name.strip(), value))
    return pairs


def parseNames(spec: typing.Optional[str]) -> typing.List[str]:
    """'a,b' -> ['a','b']"""
    return [each.strip() for each in (spec or "").split(",") if each.strip()]


def eolOf(name: typing.Optional[str]) -> str:
    """'lf' | 'crlf' | 'cr' -> the terminator. Default lf."""
    try:
        return EOL_NAMES[(name or "lf").lower()]
    except KeyError:
        raise FileTransferError(f"unknown line terminator {name!r}; choose from {sorted(EOL_NAMES)}")


def readClientConf(path: typing.Optional[str]) -> typing.Dict[str, str]:
    """A librdkafka properties file: 'name=value' lines; '#' starts a comment. For security settings."""
    conf: typing.Dict[str, str] = {}
    if not path:
        return conf
    for lineNu, raw in enumerate(pathlib.Path(path).expanduser().read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise FileTransferError(f"{path}:{lineNu}: expected name=value")
        name, value = line.split("=", 1)
        conf[name.strip()] = value.strip()
    return conf


# --------------------------------------------------------------------------------------------
# Paths: relative, logical, '/'-separated; the last component is the file name
# --------------------------------------------------------------------------------------------

def safeRelPath(path: str) -> str:
    """Return path normalized, or raise UnsafePathError.

    Guards against mistakes (an absolute path, '..', an empty component), not against a hostile
    sender: senders are assumed benevolent. placeFile() additionally refuses a result that
    resolves outside the output directory.
    """
    if not path or "\x00" in path or "\\" in path:
        raise UnsafePathError(f"unusable path {path!r}")
    if path.startswith("/"):
        raise UnsafePathError(f"path must be relative: {path!r}")
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise UnsafePathError(f"path has an empty, '.' or '..' component: {path!r}")
    return "/".join(parts)


def renderPathTemplate(template: str, bof: typing.Dict[str, typing.Any]) -> str:
    """Fill {name} from the BOF: its attrs, plus {path}, {dir} and {name} of the sent path."""
    path = bof["path"]
    fields: typing.Dict[str, str] = dict(bof.get("attrs", {}))
    fields["path"] = path
    fields["name"] = path.rsplit("/", 1)[-1]
    fields["dir"] = path.rsplit("/", 1)[0] if "/" in path else ""
    try:
        return safeRelPath(template.format_map(fields))
    except KeyError as missing:
        raise FileTransferError(f"path template uses {missing}, which this file does not carry "
                                f"(has: {sorted(fields)})")


# --------------------------------------------------------------------------------------------
# Sender side: file -> headed records
# --------------------------------------------------------------------------------------------

@dataclasses.dataclass
class Record:
    """One Kafka record to produce."""
    key: typing.Optional[bytes]
    value: bytes
    headers: typing.List[typing.Tuple[str, bytes]]


def _splitLines(data: bytes, eol: bytes) -> typing.Tuple[typing.List[bytes], bool]:
    """(lines without terminator, whether the data ended with one). Rebuild is exact by construction."""
    lines = data.split(eol)
    finalEol = data.endswith(eol)
    if finalEol:
        lines.pop()
    return lines, finalEol


def declare(path: pathlib.Path, eol: str = "\n") -> typing.Dict[str, typing.Any]:
    """Read a file once: what a sender declares about it (bytes, rows, sha256, eol, finalEol)."""
    data = pathlib.Path(path).read_bytes()
    lines, finalEol = _splitLines(data, eol.encode())
    if not data or not lines:
        raise FileTransferError(f"{path}: empty file; only non-empty files are sent")
    return {"bytes": len(data), "rows": len(lines), "sha256": hashlib.sha256(data).hexdigest(),
            "eol": eol, "finalEol": finalEol}


def iterRecords(
        path: pathlib.Path,
        relPath: str,
        key: typing.Optional[bytes] = None,
        headers: typing.Sequence[typing.Tuple[str, str]] = (),
        attrs: typing.Optional[typing.Dict[str, str]] = None,
        idHeaders: typing.Sequence[str] = (),
        eol: str = "\n",
        files: bool = True,
        _afterDeclare: typing.Optional[typing.Callable[[], None]] = None,
) -> typing.Iterator[Record]:
    """Yield the records of one file. Raises FileChangedError before yielding the last record
    if the file no longer matches what was declared: the caller then aborts its transaction.

    files=False is records mode: just the headed lines. files=True adds kfx-bof / kfx-eof.
    """
    relPath = safeRelPath(relPath)
    userHeaders = [(name, value.encode()) for name, value in headers]
    headerNames = {name for name, _ in headers}
    for name in idHeaders:
        if name not in headerNames:
            raise FileTransferError(f"idHeader {name!r} is not among the headers {sorted(headerNames)}")

    declared = declare(path, eol)
    if _afterDeclare is not None:           # a test hook: change the file between the two reads
        _afterDeclare()

    data = pathlib.Path(path).read_bytes()  # the second read: what is actually sent
    lines, finalEol = _splitLines(data, eol.encode())
    digest = hashlib.sha256(data).hexdigest()
    observed = {"bytes": len(data), "rows": len(lines), "sha256": digest}
    if (observed["bytes"], observed["rows"], observed["sha256"]) != \
            (declared["bytes"], declared["rows"], declared["sha256"]) or finalEol != declared["finalEol"]:
        raise FileChangedError(f"{path} changed while being sent "
                               f"(declared {declared['bytes']} bytes {declared['sha256'][:12]}, "
                               f"now {observed['bytes']} bytes {digest[:12]})")

    bof = {"v": FORMAT_VERSION, "path": relPath, "attrs": dict(attrs or {}), **declared,
           "idHeaders": list(idHeaders), "hdr": {"mode": "kafka"}}
    eof = {"v": FORMAT_VERSION, "path": relPath, **observed}

    last = len(lines) - 1
    for index, line in enumerate(lines):
        recHeaders = list(userHeaders)
        if files and index == 0:
            recHeaders.append((HDR_BOF, json.dumps(bof, sort_keys=True).encode()))
        if files and index == last:
            recHeaders.append((HDR_EOF, json.dumps(eof, sort_keys=True).encode()))
        yield Record(key=key, value=line, headers=recHeaders)


# --------------------------------------------------------------------------------------------
# Receiver side: records -> files
# --------------------------------------------------------------------------------------------

@dataclasses.dataclass
class CompletedFile:
    """A rebuilt, verified file."""
    path: str
    attrs: typing.Dict[str, str]
    data: bytes
    sha256: str
    bof: typing.Dict[str, typing.Any]


@dataclasses.dataclass
class _Open:
    bof: typing.Dict[str, typing.Any]
    idKey: typing.Tuple[str, ...]
    lines: typing.List[bytes]
    startedAt: float
    firstOffsets: typing.Dict[typing.Tuple[str, int], int]


class FileAssembler:
    """Collects records into files. Pure: no Kafka, no disk.

    feed() takes one record at a time, in partition order, and returns a CompletedFile when a
    record carrying kfx-eof completes a file that verifies. Files that interleave in a partition
    are told apart by the values of the headers their BOF names in idHeaders.
    """

    def __init__(self) -> None:
        self._open: typing.Dict[typing.Tuple[typing.Tuple[str, ...], typing.Tuple[str, ...]], _Open] = {}
        self.orphans = 0          # records that belong to no file we saw begin
        self.discarded: typing.List[str] = []   # paths of files dropped incomplete or unverifiable

    @staticmethod
    def _idKey(idHeaders: typing.Sequence[str], hdrs: typing.Dict[str, bytes]) -> typing.Optional[typing.Tuple[str, ...]]:
        try:
            return tuple(hdrs[name].decode() for name in idHeaders)
        except KeyError:
            return None

    def feed(
            self,
            value: bytes,
            headers: typing.Dict[str, bytes],
            topic: str = "",
            partition: int = 0,
            offset: int = 0,
            now: typing.Optional[float] = None,
    ) -> typing.Optional[CompletedFile]:
        """Take one record. Raises VerifyError (never anything else) for a record or marker that cannot
        be trusted, so a caller can reject that one file and carry on with the stream."""
        try:
            return self._feed(value, headers, topic, partition, offset, now)
        except VerifyError:
            raise
        except FileTransferError as problem:     # e.g. UnsafePathError from a marker
            raise VerifyError(str(problem)) from problem
        except (KeyError, TypeError, ValueError, AttributeError) as problem:
            raise VerifyError(f"malformed file marker: {type(problem).__name__}: {problem}") from problem

    def _feed(
            self,
            value: bytes,
            headers: typing.Dict[str, bytes],
            topic: str,
            partition: int,
            offset: int,
            now: typing.Optional[float],
    ) -> typing.Optional[CompletedFile]:
        now = time.time() if now is None else now
        state: typing.Optional[_Open] = None
        slot: typing.Optional[typing.Tuple[typing.Tuple[str, ...], typing.Tuple[str, ...]]] = None

        if HDR_BOF in headers:
            bof = self._parseMarker(headers[HDR_BOF], "BOF")
            safeRelPath(bof["path"])             # a bad path is refused before the file is buffered
            idHeaders = tuple(bof.get("idHeaders", []))
            idKey = self._idKey(idHeaders, headers)
            if idKey is None:
                raise VerifyError(f"BOF of {bof.get('path')!r} names idHeaders {list(idHeaders)} "
                                  f"that its first record does not carry")
            slot = (idHeaders, idKey)
            if slot in self._open:       # an earlier file of the same identity never finished
                self.discarded.append(self._open[slot].bof["path"])
            state = self._open[slot] = _Open(bof=bof, idKey=idKey, lines=[], startedAt=now, firstOffsets={})
        else:
            for candidate, each in self._open.items():
                if self._idKey(candidate[0], headers) == candidate[1]:
                    slot, state = candidate, each
                    break

        if state is None or slot is None:
            self.orphans += 1
            return None

        state.lines.append(value)
        state.firstOffsets.setdefault((topic, partition), offset)

        if HDR_EOF not in headers:
            return None

        del self._open[slot]
        return self._complete(state, self._parseMarker(headers[HDR_EOF], "EOF"))

    @staticmethod
    def _parseMarker(raw: bytes, which: str) -> typing.Dict[str, typing.Any]:
        try:
            marker = json.loads(raw)
        except ValueError as problem:
            raise VerifyError(f"{which} marker is not JSON: {problem}")
        if marker.get("v") != FORMAT_VERSION:
            raise VerifyError(f"{which} marker has format version {marker.get('v')!r}; this reads {FORMAT_VERSION}")
        return marker

    def _complete(self, state: _Open, eof: typing.Dict[str, typing.Any]) -> CompletedFile:
        bof = state.bof
        path = bof["path"]
        eol = bof["eol"].encode()
        data = eol.join(state.lines) + (eol if bof["finalEol"] else b"")
        digest = hashlib.sha256(data).hexdigest()
        problems = []
        if eof.get("path") != path:
            problems.append(f"EOF names {eof.get('path')!r}")
        for name, got in (("rows", len(state.lines)), ("bytes", len(data)), ("sha256", digest)):
            if bof.get(name) != got:
                problems.append(f"BOF {name}={bof.get(name)!r}, rebuilt {got!r}")
            if eof.get(name) != got:
                problems.append(f"EOF {name}={eof.get(name)!r}, rebuilt {got!r}")
        if problems:
            self.discarded.append(path)
            raise VerifyError(f"{path}: " + "; ".join(problems))
        return CompletedFile(path=safeRelPath(path), attrs=bof.get("attrs", {}), data=data, sha256=digest, bof=bof)

    def dropStale(self, maxAgeSec: float, now: typing.Optional[float] = None) -> typing.List[str]:
        """Forget files that began more than maxAgeSec ago and never ended (aborted, or a late reader)."""
        now = time.time() if now is None else now
        gone = [slot for slot, each in self._open.items() if now - each.startedAt > maxAgeSec]
        paths = []
        for slot in gone:
            paths.append(self._open[slot].bof["path"])
            del self._open[slot]
        self.discarded.extend(paths)
        return paths

    def openCount(self) -> int:
        return len(self._open)

    def safeOffsets(self) -> typing.Dict[typing.Tuple[str, int], int]:
        """Per (topic, partition), the lowest offset of any still-open file: committing at or below
        this loses nothing if we crash, because those files would be read again."""
        low: typing.Dict[typing.Tuple[str, int], int] = {}
        for each in self._open.values():
            for tp, off in each.firstOffsets.items():
                low[tp] = min(off, low.get(tp, off))
        return low


# --------------------------------------------------------------------------------------------
# Disk: atomic placement
# --------------------------------------------------------------------------------------------

def placeFile(
        outBase: pathlib.Path,
        relPath: str,
        data: bytes,
        sha256: str,
        onExists: str = "replace",
        latest: typing.Optional[str] = None,
) -> str:
    """Write data at outBase/relPath atomically. Returns 'written', 'replaced' or 'skipped'.

    Same path and same content: skipped. Same path, other content: replaced (onExists=replace),
    or FileTransferError (fail), or left alone (skip). latest=NAME also points the symlink
    <dir of the file's directory>/NAME at the file's directory.
    """
    if onExists not in ("replace", "fail", "skip"):
        raise FileTransferError(f"--onExists must be replace, fail or skip, not {onExists!r}")
    base = pathlib.Path(outBase).expanduser().resolve()
    target = (base / safeRelPath(relPath))
    if base not in target.resolve().parents:
        raise UnsafePathError(f"{relPath!r} resolves outside {base}")

    status = "written"
    if target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest() == sha256:
            status = "skipped"
        elif onExists == "fail":
            raise FileTransferError(f"{target} exists with other content")
        elif onExists == "skip":
            status = "skipped"
        else:
            status = "replaced"

    if status != "skipped":
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.tmp-{os.getpid()}")
        with open(tmp, "wb") as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, target)

    if latest and target.parent != base:
        link = target.parent.parent / latest
        tmpLink = link.with_name(f".{latest}.tmp-{os.getpid()}")
        if tmpLink.is_symlink() or tmpLink.exists():
            tmpLink.unlink()
        os.symlink(target.parent.name, tmpLink)
        os.replace(tmpLink, link)
    return status


# --------------------------------------------------------------------------------------------
# Kafka: the only part that needs a broker (and the client library)
# --------------------------------------------------------------------------------------------

def _client() -> typing.Any:
    try:
        import confluent_kafka
    except ImportError as problem:
        raise FileTransferError(
            "confluent-kafka is not installed. For Kerberos, build it against the library kafka-sbom.pcs "
            "provides:  . /opt/kafka/librdkafka/build-env.sh ; pip install confluent-kafka") from problem
    return confluent_kafka


def sendFiles(
        files: typing.Sequence[typing.Tuple[pathlib.Path, str]],
        bootstrap: str,
        topic: str,
        key: typing.Optional[str] = None,
        headers: typing.Sequence[typing.Tuple[str, str]] = (),
        attrs: typing.Optional[typing.Dict[str, str]] = None,
        idHeaders: typing.Sequence[str] = (),
        eol: str = "\n",
        filesMode: bool = True,
        clientConf: typing.Optional[typing.Dict[str, str]] = None,
        timeoutSec: float = 30.0,
        say: typing.Callable[[str], None] = print,
        _afterDeclare: typing.Optional[typing.Callable[[], None]] = None,
) -> typing.List[typing.Tuple[str, str]]:
    """Send each (path, relPath) as one transaction. Returns [(relPath, 'sent'|'failed: why')].

    A file that fails is aborted (nothing becomes visible to read_committed readers) and the next
    one is tried; the caller decides what a failure means overall.
    """
    ck = _client()
    conf = {
        "bootstrap.servers": bootstrap,
        "transactional.id": f"kfx-{socket.gethostname()}-{os.getpid()}-{int(time.time())}",
        "transaction.timeout.ms": int(max(timeoutSec, 10) * 1000),
        "linger.ms": 20,
        "batch.size": 262144,
        "queue.buffering.max.messages": 500000,
        **(clientConf or {}),
    }
    producer = ck.Producer(conf)
    producer.init_transactions(timeoutSec)
    results: typing.List[typing.Tuple[str, str]] = []
    keyBytes = key.encode() if key is not None else None

    for path, relPath in files:
        deliveryErrors: typing.List[str] = []

        def onDelivery(err: typing.Any, _msg: typing.Any) -> None:
            if err is not None:
                deliveryErrors.append(str(err))

        try:
            producer.begin_transaction()
            count = 0
            for rec in iterRecords(path, relPath, keyBytes, headers, attrs, idHeaders, eol,
                                   files=filesMode, _afterDeclare=_afterDeclare):
                while True:
                    try:
                        producer.produce(topic, key=rec.key, value=rec.value, headers=rec.headers,
                                         on_delivery=onDelivery)
                        break
                    except BufferError:
                        producer.poll(0.05)
                count += 1
                if count % 1000 == 0:
                    producer.poll(0)
            producer.commit_transaction(timeoutSec)
            if deliveryErrors:
                raise FileTransferError("delivery failed: " + "; ".join(sorted(set(deliveryErrors))))
            results.append((relPath, "sent"))
            say(f"sent {relPath}: {count} records in one transaction")
        except (FileTransferError, ck.KafkaException) as problem:
            try:
                producer.abort_transaction(timeoutSec)
            except ck.KafkaException:
                pass
            results.append((relPath, f"failed: {problem}"))
            say(f"FAILED {relPath}: {problem} (transaction aborted; nothing became visible)")
    return results


def _commitSafe(consumer: typing.Any, assembler: FileAssembler, ck: typing.Any,
                lastSeen: typing.Dict[typing.Tuple[str, int], int]) -> None:
    """Commit up to the oldest open file's first record, or past the last record seen if none open."""
    offsets = {tp: off + 1 for tp, off in lastSeen.items()}
    offsets.update(assembler.safeOffsets())
    if offsets:
        consumer.commit(offsets=[ck.TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                        asynchronous=False)


def receiveFiles(
        bootstrap: str,
        topics: typing.Sequence[str],
        group: str,
        outBase: pathlib.Path,
        pathTemplate: typing.Optional[str] = None,
        onExists: str = "replace",
        latest: typing.Optional[str] = None,
        clientConf: typing.Optional[typing.Dict[str, str]] = None,
        idleExitSec: typing.Optional[float] = None,
        maxFiles: typing.Optional[int] = None,
        staleSec: float = 300.0,
        say: typing.Callable[[str], None] = print,
        stop: typing.Callable[[], bool] = lambda: False,
) -> typing.Dict[str, int]:
    """Read topics with isolation read_committed and rebuild files under outBase.

    Runs until stop() is true, or idleExitSec seconds pass with nothing to read, or maxFiles files
    were written. Returns counts: written, replaced, skipped, failed, orphans.
    """
    ck = _client()
    consumer = ck.Consumer({
        "bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "earliest",
        "isolation.level": "read_committed", "enable.auto.commit": False,
        **(clientConf or {}),
    })
    consumer.subscribe(list(topics))
    assembler = FileAssembler()
    counts = {"written": 0, "replaced": 0, "skipped": 0, "failed": 0, "orphans": 0}
    lastSeen: typing.Dict[typing.Tuple[str, int], int] = {}
    lastActivity = time.time()
    try:
        while not stop():
            msg = consumer.poll(1.0)
            if msg is None:
                if idleExitSec is not None and time.time() - lastActivity > idleExitSec:
                    break
                for gone in assembler.dropStale(staleSec):
                    say(f"dropped {gone}: began but never ended")
                continue
            if msg.error():
                if msg.error().code() == ck.KafkaError._PARTITION_EOF:
                    continue
                raise FileTransferError(f"consumer error: {msg.error()}")
            lastActivity = time.time()
            tp = (msg.topic(), msg.partition())
            lastSeen[tp] = msg.offset()
            hdrs = {name: value for name, value in (msg.headers() or [])}
            try:
                done = assembler.feed(msg.value() or b"", hdrs, msg.topic(), msg.partition(), msg.offset())
            except VerifyError as problem:
                counts["failed"] += 1
                say(f"REJECTED: {problem}")
                continue
            except Exception as problem:         # a bug must not become a poison pill either
                counts["failed"] += 1
                say(f"REJECTED (unexpected {type(problem).__name__}): {problem}")
                continue
            if done is None:
                continue
            try:
                rel = renderPathTemplate(pathTemplate, done.bof) if pathTemplate else done.path
                status = placeFile(outBase, rel, done.data, done.sha256, onExists, latest)
            except FileTransferError as problem:
                counts["failed"] += 1
                say(f"REJECTED {done.path}: {problem}")
                continue
            counts[status] += 1
            say(f"{status} {rel} ({len(done.data)} bytes, sha256 {done.sha256[:12]})")
            _commitSafe(consumer, assembler, ck, lastSeen)
            if maxFiles is not None and counts["written"] + counts["replaced"] + counts["skipped"] >= maxFiles:
                break
    finally:
        _commitSafe(consumer, assembler, ck, lastSeen)
        consumer.close()
    counts["orphans"] = assembler.orphans
    return counts
