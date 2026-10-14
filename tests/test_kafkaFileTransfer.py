"""Tests of the pure parts of bisos.kafka.kafkaFileTransfer: no broker, no Kafka client needed.

Run:  python -m unittest discover -s tests -v      (from the repo root, in the bisos3 venv)
"""
import hashlib
import itertools
import json
import pathlib
import tempfile
import unittest

from bisos.kafka import kafkaFileTransfer as kft

HDRS = [("run", "r1"), ("host", "A"), ("part", "0")]


def records(path, rel="a/b/data.txt", headers=HDRS, idHeaders=("run", "host", "part"), eol="\n", files=True, **kw):
    return list(kft.iterRecords(path, rel, b"A", headers, {"vendor": "v"}, idHeaders, eol, files=files, **kw))


def asDict(rec):
    return {name: value for name, value in rec.headers}


def rebuild(recs, assembler=None):
    assembler = assembler or kft.FileAssembler()
    done = []
    for off, rec in enumerate(recs):
        got = assembler.feed(rec.value, asDict(rec), "t", 0, off)
        if got:
            done.append(got)
    return done, assembler


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, name, data: bytes):
        p = self.dir / name
        p.write_bytes(data)
        return p


class PathTests(unittest.TestCase):
    def test_accepts(self):
        self.assertEqual(kft.safeRelPath("a/b/c.txt"), "a/b/c.txt")
        self.assertEqual(kft.safeRelPath("c.txt"), "c.txt")

    def test_rejects(self):
        for bad in ("", "/etc/passwd", "../x", "a/../x", "a//b", "a/./b", "a\\b", "a/b/", "x\x00y"):
            with self.assertRaises(kft.UnsafePathError, msg=bad):
                kft.safeRelPath(bad)

    def test_template(self):
        bof = {"path": "d1/d2/f.txt", "attrs": {"vendor": "v", "run": "r"}}
        self.assertEqual(kft.renderPathTemplate("{vendor}/{run}/{name}", bof), "v/r/f.txt")
        self.assertEqual(kft.renderPathTemplate("{dir}/x/{name}", bof), "d1/d2/x/f.txt")
        with self.assertRaises(kft.FileTransferError):
            kft.renderPathTemplate("{nope}/x", bof)
        with self.assertRaises(kft.UnsafePathError):
            kft.renderPathTemplate("../{name}", bof)


class ParseTests(unittest.TestCase):
    def test_keyvalues(self):
        self.assertEqual(kft.parseKeyValues("a=1, b=x=y"), [("a", "1"), ("b", "x=y")])
        self.assertEqual(kft.parseKeyValues(None), [])
        with self.assertRaises(kft.FileTransferError):
            kft.parseKeyValues("novalue")

    def test_eol(self):
        self.assertEqual(kft.eolOf("crlf"), "\r\n")
        with self.assertRaises(kft.FileTransferError):
            kft.eolOf("nl")


class RecordTests(Base):
    def test_records_mode_is_plain_rows(self):
        p = self.write("f", b"a|1\nb|2\nc|3\n")
        recs = records(p, files=False)
        self.assertEqual([r.value for r in recs], [b"a|1", b"b|2", b"c|3"])      # no terminator in values
        for r in recs:
            self.assertEqual(r.key, b"A")
            self.assertEqual([n for n, _ in r.headers], ["run", "host", "part"])  # no kfx headers at all

    def test_files_mode_markers_only_on_first_and_last(self):
        recs = records(self.write("f", b"a\nb\nc\n"))
        names = [[n for n, _ in r.headers] for r in recs]
        self.assertIn(kft.HDR_BOF, names[0]); self.assertNotIn(kft.HDR_EOF, names[0])
        self.assertNotIn(kft.HDR_BOF, names[1]); self.assertNotIn(kft.HDR_EOF, names[1])
        self.assertIn(kft.HDR_EOF, names[2]); self.assertNotIn(kft.HDR_BOF, names[2])
        bof = json.loads(asDict(recs[0])[kft.HDR_BOF])
        eof = json.loads(asDict(recs[2])[kft.HDR_EOF])
        self.assertEqual((bof["rows"], bof["bytes"], bof["path"]), (3, 6, "a/b/data.txt"))
        self.assertEqual(bof["sha256"], eof["sha256"])
        self.assertEqual(bof["idHeaders"], ["run", "host", "part"])
        self.assertEqual(bof["attrs"], {"vendor": "v"})

    def test_single_line_has_both_markers(self):
        recs = records(self.write("f", b"only\n"))
        self.assertEqual(len(recs), 1)
        self.assertTrue({kft.HDR_BOF, kft.HDR_EOF} <= set(asDict(recs[0])))

    def test_empty_rejected(self):
        with self.assertRaises(kft.FileTransferError):
            records(self.write("e", b""))

    def test_idheader_must_be_a_header(self):
        with self.assertRaises(kft.FileTransferError):
            records(self.write("f", b"a\n"), idHeaders=("missing",))

    def test_file_changed_during_send(self):
        p = self.write("f", b"a\nb\n")
        with self.assertRaises(kft.FileChangedError):
            records(p, _afterDeclare=lambda: p.write_bytes(b"a\nb\nc\n"))


class RoundTripTests(Base):
    CASES = {
        "lf_final": (b"a|1\nb|2\nc|3\n", "\n"),
        "lf_no_final": (b"a|1\nb|2\nc|3", "\n"),
        "crlf": (b"a|1\r\nb|2\r\n", "\r\n"),
        "one_line_no_eol": (b"x", "\n"),
        "blank_lines": (b"a\n\n\nb\n", "\n"),
        "bom_and_utf8": ("﻿héllo|wörld\n".encode(), "\n"),
        "pipes_and_equals": (b"k=v|a|b|c=d\n", "\n"),
        "cr_inside_lf_file": (b"a\rb\nc\n", "\n"),
    }

    def test_exact_bytes(self):
        for name, (data, eol) in self.CASES.items():
            with self.subTest(name):
                done, _ = rebuild(records(self.write(name, data), eol=eol))
                self.assertEqual(len(done), 1)
                self.assertEqual(done[0].data, data)
                self.assertEqual(done[0].sha256, hashlib.sha256(data).hexdigest())

    def test_interleaved_files_demultiplexed(self):
        pa = self.write("a", b"".join(b"A%d\n" % i for i in range(5)))
        pb = self.write("b", b"".join(b"B%d\n" % i for i in range(7)))
        ra = records(pa, "x/a.txt", headers=[("run", "r1"), ("host", "A")], idHeaders=("run", "host"))
        rb = records(pb, "y/b.txt", headers=[("run", "r1"), ("host", "B")], idHeaders=("run", "host"))
        mixed = [r for pair in itertools.zip_longest(ra, rb) for r in pair if r is not None]
        done, asm = rebuild(mixed)
        self.assertEqual({d.path: d.data for d in done},
                         {"x/a.txt": pa.read_bytes(), "y/b.txt": pb.read_bytes()})
        self.assertEqual((asm.orphans, asm.openCount()), (0, 0))

    def test_records_mode_data_is_orphaned_not_misfiled(self):
        done, asm = rebuild(records(self.write("f", b"a\nb\n"), files=False))
        self.assertEqual(done, [])
        self.assertEqual(asm.orphans, 2)

    def test_tampered_value_rejected(self):
        recs = records(self.write("f", b"a\nb\nc\n"))
        recs[1].value = b"X"
        with self.assertRaises(kft.VerifyError):
            rebuild(recs)

    def test_missing_middle_record_rejected(self):
        recs = records(self.write("f", b"a\nb\nc\n"))
        with self.assertRaises(kft.VerifyError):
            rebuild([recs[0], recs[2]])

    def test_marker_disagreement_rejected(self):
        recs = records(self.write("f", b"a\nb\n"))
        hdrs = dict(recs[-1].headers)
        eof = json.loads(hdrs[kft.HDR_EOF]); eof["sha256"] = "0" * 64
        recs[-1].headers = [(n, v) for n, v in recs[-1].headers if n != kft.HDR_EOF] + [(kft.HDR_EOF, json.dumps(eof).encode())]
        with self.assertRaises(kft.VerifyError):
            rebuild(recs)

    def test_unknown_marker_version_rejected(self):
        recs = records(self.write("f", b"a\n"))
        bad = json.loads(asDict(recs[0])[kft.HDR_BOF]); bad["v"] = 99
        recs[0].headers = [(n, v) for n, v in recs[0].headers if n != kft.HDR_BOF] + [(kft.HDR_BOF, json.dumps(bad).encode())]
        with self.assertRaises(kft.VerifyError):
            rebuild(recs)

    def _withBof(self, recs, **changes):
        bof = json.loads(asDict(recs[0])[kft.HDR_BOF]); bof.update(changes)
        recs[0].headers = [(n, v) for n, v in recs[0].headers if n != kft.HDR_BOF] + [(kft.HDR_BOF, json.dumps(bof).encode())]
        return recs

    def test_hostile_path_in_marker_is_a_verify_error_and_buffers_nothing(self):
        for bad in ("../x", "/etc/passwd", "a/../../b", "a//b"):
            with self.subTest(bad):
                recs = self._withBof(records(self.write("f", b"a\n")), path=bad)
                asm = kft.FileAssembler()
                with self.assertRaises(kft.VerifyError):
                    asm.feed(recs[0].value, asDict(recs[0]))
                self.assertEqual(asm.openCount(), 0)

    def test_malformed_marker_is_a_verify_error_not_a_crash(self):
        recs = records(self.write("f", b"a\n"))
        bof = json.loads(asDict(recs[0])[kft.HDR_BOF]); del bof["eol"]
        cut = [(n, v) for n, v in recs[0].headers if n != kft.HDR_BOF] + [(kft.HDR_BOF, json.dumps(bof).encode())]
        with self.assertRaises(kft.VerifyError):
            kft.FileAssembler().feed(recs[0].value, dict(cut))
        with self.assertRaises(kft.VerifyError):
            kft.FileAssembler().feed(b"x", {kft.HDR_BOF: b"{not json", "run": b"r1"})

    def test_stream_continues_after_a_rejected_file(self):
        asm = kft.FileAssembler()
        bad = self._withBof(records(self.write("bad", b"a\n")), path="../x")
        with self.assertRaises(kft.VerifyError):
            asm.feed(bad[0].value, asDict(bad[0]))
        done, _ = rebuild(records(self.write("good", b"g\n"), "ok/g.txt"), asm)
        self.assertEqual([d.path for d in done], ["ok/g.txt"])

    def test_new_bof_discards_unfinished_file_of_same_identity(self):
        p = self.write("f", b"a\nb\nc\n")
        first = records(p)
        done, asm = rebuild(first[:2])           # begins, never ends
        self.assertEqual((done, asm.openCount()), ([], 1))
        done, asm = rebuild(records(p), asm)     # the same identity again
        self.assertEqual([d.path for d in done], ["a/b/data.txt"])
        self.assertEqual(asm.discarded, ["a/b/data.txt"])

    def test_stale_and_safe_offsets(self):
        recs = records(self.write("f", b"a\nb\nc\n"))
        asm = kft.FileAssembler()
        asm.feed(recs[0].value, asDict(recs[0]), "t", 0, 10, now=1000.0)
        asm.feed(recs[1].value, asDict(recs[1]), "t", 0, 11, now=1000.0)
        self.assertEqual(asm.safeOffsets(), {("t", 0): 10})
        self.assertEqual(asm.dropStale(300, now=1100.0), [])
        self.assertEqual(asm.dropStale(300, now=1400.0), ["a/b/data.txt"])
        self.assertEqual(asm.safeOffsets(), {})


class PlaceTests(Base):
    def sha(self, data):
        return hashlib.sha256(data).hexdigest()

    def test_write_skip_replace_fail(self):
        out = self.dir / "out"
        self.assertEqual(kft.placeFile(out, "d/f.txt", b"one", self.sha(b"one")), "written")
        self.assertEqual(kft.placeFile(out, "d/f.txt", b"one", self.sha(b"one")), "skipped")
        self.assertEqual(kft.placeFile(out, "d/f.txt", b"two", self.sha(b"two")), "replaced")
        self.assertEqual((out / "d/f.txt").read_bytes(), b"two")
        with self.assertRaises(kft.FileTransferError):
            kft.placeFile(out, "d/f.txt", b"three", self.sha(b"three"), onExists="fail")
        self.assertEqual(kft.placeFile(out, "d/f.txt", b"three", self.sha(b"three"), onExists="skip"), "skipped")
        self.assertEqual((out / "d/f.txt").read_bytes(), b"two")
        self.assertEqual([p.name for p in (out / "d").iterdir()], ["f.txt"])     # no temp file left

    def test_refuses_to_leave_outbase(self):
        out = self.dir / "out"
        out.mkdir()
        (out / "link").symlink_to(self.dir)                                       # a symlink pointing outside
        with self.assertRaises(kft.UnsafePathError):
            kft.placeFile(out, "link/f.txt", b"x", self.sha(b"x"))
        with self.assertRaises(kft.UnsafePathError):
            kft.placeFile(out, "../f.txt", b"x", self.sha(b"x"))
        self.assertFalse((self.dir / "f.txt").exists())

    def test_latest_symlink_moves(self):
        out = self.dir / "out"
        kft.placeFile(out, "tech/run1/r.txt", b"1", self.sha(b"1"), latest="latest")
        self.assertEqual((out / "tech/latest").resolve(), (out / "tech/run1").resolve())
        kft.placeFile(out, "tech/run2/r.txt", b"2", self.sha(b"2"), latest="latest")
        self.assertEqual((out / "tech/latest").resolve(), (out / "tech/run2").resolve())
        self.assertEqual((out / "tech/latest/r.txt").read_bytes(), b"2")


if __name__ == "__main__":
    unittest.main()
