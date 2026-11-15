"""质量放行单元测试（仅依赖标准库）：
python3 -m unittest discover -s tests -v

覆盖：CSV 解析与整批拒绝、对照评估口径、放行依据失效规则、
存储层批次替换/放行事务、可信下载过滤。
"""
import csv
import io
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.quality import (ANOMALY, NORMAL, PENDING, FormatError,
                         build_download_csv, build_view, evaluate,
                         parse_csv, parse_hour, parse_threshold)
from app.store import (NoActiveBatchError, NotAnomalyError, ObsNotFoundError,
                       QualityStore, StaleBasisError)

HEADER = "观测ID,站点,UTC小时,分组,读数"


def csv_bytes(lines, header=HEADER):
    return (header + "\n" + "\n".join(lines) + "\n").encode("utf-8")


def sample_rows():
    """4 站同组同时：S4 异常；另 1 组只有 2 站：待补。"""
    return parse_csv(csv_bytes([
        "A1,S1,2026-11-14T10:00Z,G,10.0",
        "A2,S2,2026-11-14T10:00Z,G,11.0",
        "A3,S3,2026-11-14T10:00Z,G,12.0",
        "A4,S4,2026-11-14T10:00Z,G,99.0",
        "B1,S1,2026-11-14T11:00Z,G,10.0",
        "B2,S2,2026-11-14T11:00Z,G,10.5",
    ]))


def evaluated_sample(threshold=5.0):
    rows, summary = evaluate(sample_rows(), threshold)
    return rows, summary


class TestParseCsv(unittest.TestCase):
    def test_ok_chinese_header(self):
        rows = parse_csv(csv_bytes(["A1,S1,2026-11-14T10:00Z,G,10.5"]))
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["obs_id"], r["station"], r["hour"], r["group"], r["reading"]),
                         ("A1", "S1", "2026-11-14T10:00Z", "G", 10.5))

    def test_ok_english_header_and_bom(self):
        raw = "﻿obs_id,station,utc_hour,group,reading\nA1,S1,2026-11-14 10:00,G,1\n"
        rows = parse_csv(raw.encode("utf-8"))
        self.assertEqual(rows[0]["hour"], "2026-11-14T10:00Z")

    def test_empty_and_header_only(self):
        with self.assertRaises(FormatError):
            parse_csv(b"")
        with self.assertRaises(FormatError):
            parse_csv((HEADER + "\n").encode())

    def test_unknown_column_rejected(self):
        with self.assertRaisesRegex(FormatError, "无法识别的列"):
            parse_csv(csv_bytes(["A1,S1,2026-11-14T10:00Z,G,1,extra"],
                                header=HEADER + ",备注"))

    def test_missing_column_rejected(self):
        with self.assertRaisesRegex(FormatError, "缺少必需列"):
            parse_csv(b"obs_id,station,utc_hour,reading\nA1,S1,2026-11-14T10:00Z,1\n")

    def test_duplicate_station_hour_rejects_batch(self):
        with self.assertRaisesRegex(FormatError, "同站同小时重复"):
            parse_csv(csv_bytes([
                "A1,S1,2026-11-14T10:00Z,G,1",
                "A2,S1,2026-11-14T10:00Z,G2,2",  # 不同分组也算重复
            ]))

    def test_duplicate_obs_id_rejects_batch(self):
        with self.assertRaisesRegex(FormatError, "重复"):
            parse_csv(csv_bytes([
                "A1,S1,2026-11-14T10:00Z,G,1",
                "A1,S2,2026-11-14T10:00Z,G,2",
            ]))

    def test_bad_reading_and_field_count(self):
        with self.assertRaisesRegex(FormatError, "不是数值"):
            parse_csv(csv_bytes(["A1,S1,2026-11-14T10:00Z,G,abc"]))
        with self.assertRaisesRegex(FormatError, "有限数值"):
            parse_csv(csv_bytes(["A1,S1,2026-11-14T10:00Z,G,nan"]))
        with self.assertRaisesRegex(FormatError, "字段数"):
            parse_csv(csv_bytes(["A1,S1,2026-11-14T10:00Z,G"]))


class TestParseHour(unittest.TestCase):
    def test_forms(self):
        self.assertEqual(parse_hour("2026-11-14T10:00Z"), "2026-11-14T10:00Z")
        self.assertEqual(parse_hour("2026-11-14 10:00"), "2026-11-14T10:00Z")
        self.assertEqual(parse_hour("2026-11-14T10:00:00+00:00"), "2026-11-14T10:00Z")
        self.assertEqual(parse_hour("2026-11-14T18:00+08:00"), "2026-11-14T10:00Z")

    def test_not_whole_hour(self):
        with self.assertRaisesRegex(FormatError, "整点"):
            parse_hour("2026-11-14T10:30Z")
        with self.assertRaisesRegex(FormatError, "整点"):
            parse_hour("2026-11-14T10:30+08:00")  # 换算后 02:30Z 仍非整点

    def test_garbage(self):
        with self.assertRaises(FormatError):
            parse_hour("not-a-time")


class TestThreshold(unittest.TestCase):
    def test_ok(self):
        self.assertEqual(parse_threshold("5"), 5.0)
        self.assertEqual(parse_threshold("0"), 0.0)
        self.assertEqual(parse_threshold("2.5"), 2.5)

    def test_bad(self):
        for raw in ("", "abc", "-1", "nan", "inf"):
            with self.assertRaises(FormatError, msg=raw):
                parse_threshold(raw)


class TestEvaluate(unittest.TestCase):
    def test_statuses(self):
        rows, summary = evaluated_sample()
        by_id = {r["obs_id"]: r for r in rows}
        # S4=99，其余 10/11/12 中位数 11，差 88 > 5 => 异常
        self.assertEqual(by_id["A4"]["status"], ANOMALY)
        self.assertEqual(by_id["A4"]["peers"], 3)
        self.assertEqual(by_id["A4"]["median"], 11.0)
        # 正常行：S1=10，其他 11/12/99 中位数 12，差 2 <= 5
        self.assertEqual(by_id["A1"]["status"], NORMAL)
        self.assertEqual(by_id["A1"]["median"], 12.0)
        # 只有 1 个参照站 => 待补
        self.assertEqual(by_id["B1"]["status"], PENDING)
        self.assertEqual(by_id["B1"]["peers"], 1)
        self.assertEqual(summary, {"total": 6, "normal": 3, "anomaly": 1,
                                   "pending": 2, "threshold": 5.0})

    def test_threshold_boundary_not_exceeded(self):
        # 差恰好等于阈值 => 不超阈值 => 正常
        rows = parse_csv(csv_bytes([
            "A1,S1,2026-11-14T10:00Z,G,15",
            "A2,S2,2026-11-14T10:00Z,G,10",
            "A3,S3,2026-11-14T10:00Z,G,10",
        ]))
        out, _ = evaluate(rows, 5.0)
        self.assertEqual(out[0]["status"], NORMAL)
        out2, _ = evaluate(rows, 4.9)
        self.assertEqual(out2[0]["status"], ANOMALY)

    def test_zero_peers_pending(self):
        rows = parse_csv(csv_bytes(["A1,S1,2026-11-14T10:00Z,G,1"]))
        out, _ = evaluate(rows, 1.0)
        self.assertEqual(out[0]["status"], PENDING)
        self.assertIsNone(out[0]["median"])

    def test_groups_and_hours_do_not_cross(self):
        # 同站可在不同小时出现；对照只发生在同组同小时内
        rows = parse_csv(csv_bytes([
            "A1,S1,2026-11-14T10:00Z,G,100",
            "A2,S2,2026-11-14T10:00Z,H,100",   # 不同组
            "A3,S3,2026-11-14T11:00Z,G,100",   # 不同小时
        ]))
        out, _ = evaluate(rows, 1.0)
        self.assertTrue(all(r["status"] == PENDING for r in out))


class TestBasis(unittest.TestCase):
    def basis_of(self, rows, threshold, obs_id):
        out, _ = evaluate(rows, threshold)
        return next(r["basis"] for r in out if r["obs_id"] == obs_id)

    def test_identical_reupload_keeps_basis(self):
        b1 = self.basis_of(sample_rows(), 5.0, "A4")
        b2 = self.basis_of(sample_rows(), 5.0, "A4")
        self.assertEqual(b1, b2)

    def test_own_value_change_invalidates(self):
        rows2 = [dict(r) for r in sample_rows()]
        rows2[3]["reading"] = 88.0  # A4 自身读数变化
        self.assertNotEqual(self.basis_of(sample_rows(), 5.0, "A4"),
                            self.basis_of(rows2, 5.0, "A4"))

    def test_peer_change_invalidates(self):
        rows2 = [dict(r) for r in sample_rows()]
        rows2[0]["reading"] = 10.6  # 参与对照的 S1 读数变化
        self.assertNotEqual(self.basis_of(sample_rows(), 5.0, "A4"),
                            self.basis_of(rows2, 5.0, "A4"))

    def test_peer_add_remove_invalidates(self):
        base = sample_rows()
        extra = base + [{"obs_id": "A5", "station": "S5",
                         "hour": "2026-11-14T10:00Z", "group": "G", "reading": 11.5}]
        self.assertNotEqual(self.basis_of(base, 5.0, "A4"),
                            self.basis_of(extra, 5.0, "A4"))
        removed = [r for r in base if r["obs_id"] != "A1"]
        self.assertNotEqual(self.basis_of(base, 5.0, "A4"),
                            self.basis_of(removed, 5.0, "A4"))

    def test_threshold_change_invalidates(self):
        self.assertNotEqual(self.basis_of(sample_rows(), 5.0, "A4"),
                            self.basis_of(sample_rows(), 6.0, "A4"))

    def test_unrelated_row_change_keeps_basis(self):
        # 不同小时/分组的记录变化不影响 A4 的依据
        rows2 = [dict(r) for r in sample_rows()]
        rows2[4]["reading"] = 77.0  # B1（另一小时）
        self.assertEqual(self.basis_of(sample_rows(), 5.0, "A4"),
                         self.basis_of(rows2, 5.0, "A4"))


class QualityStoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = QualityStore(os.path.join(self.tmp.name, "q.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def publish(self, rows=None, threshold=5.0, name="批次"):
        rows = rows if rows is not None else sample_rows()
        evaluated, summary = evaluate(rows, threshold)
        payload = {"name": name, "threshold": threshold,
                   "rows": evaluated, "summary": summary}
        return self.store.publish_batch(name, threshold, payload)

    def basis(self, obs_id):
        cur = self.store.current()
        return next(r["basis"] for r in cur["rows"] if r["obs_id"] == obs_id)


class TestQualityStore(QualityStoreCase):
    def test_publish_and_current(self):
        self.assertIsNone(self.store.current())
        bid = self.publish()
        cur = self.store.current()
        self.assertEqual(cur["id"], bid)
        self.assertEqual(cur["summary"]["anomaly"], 1)
        self.assertEqual(cur["summary"]["trusted"], 3)  # 3 正常，异常未放行，待补不算

    def test_failed_record_keeps_active(self):
        bid = self.publish()
        self.store.record_failed("坏批次", "同站同小时重复")
        cur = self.store.current()
        self.assertEqual(cur["id"], bid)
        states = [h["state"] for h in self.store.history()]
        self.assertEqual(states.count("active"), 1)
        self.assertIn("failed", states)

    def test_release_happy_path(self):
        self.publish()
        res = self.store.release("A4", self.basis("A4"), "人工确认有效")
        self.assertEqual(res["obs_id"], "A4")
        cur = self.store.current()
        row = next(r for r in cur["rows"] if r["obs_id"] == "A4")
        self.assertTrue(row["release"]["valid"])
        self.assertTrue(row["trusted"])
        self.assertEqual(cur["summary"]["released_valid"], 1)
        self.assertEqual(cur["summary"]["trusted"], 4)

    def test_release_requires_active_batch(self):
        with self.assertRaises(NoActiveBatchError):
            self.store.release("A4", "x", "")

    def test_release_unknown_obs(self):
        self.publish()
        with self.assertRaises(ObsNotFoundError):
            self.store.release("ZZZ", "x", "")

    def test_release_stale_basis_rejected(self):
        self.publish()
        with self.assertRaises(StaleBasisError):
            self.store.release("A4", "0" * 64, "")

    def test_release_only_anomaly(self):
        self.publish()
        with self.assertRaises(NotAnomalyError):
            self.store.release("A1", self.basis("A1"), "")   # 正常
        with self.assertRaises(NotAnomalyError):
            self.store.release("B1", self.basis("B1"), "")   # 待补

    def test_reupload_invalidates_changed_keeps_others(self):
        # 两个不同小时的异常 A4、C3；重传只改 A4 的参照 => A4 放行失效，C3 放行保留
        rows1 = parse_csv(csv_bytes([
            "A1,S1,2026-11-14T10:00Z,G,10",
            "A2,S2,2026-11-14T10:00Z,G,11",
            "A3,S3,2026-11-14T10:00Z,G,12",
            "A4,S4,2026-11-14T10:00Z,G,99",
            "C1,S1,2026-11-14T12:00Z,G,10",
            "C2,S2,2026-11-14T12:00Z,G,10",
            "C3,S3,2026-11-14T12:00Z,G,50",
        ]))
        self.publish(rows1)
        old_basis_a4 = self.basis("A4")
        self.store.release("A4", old_basis_a4, "第一批放行")
        self.store.release("C3", self.basis("C3"), "第一批放行")
        # 重传：A4 的参照 S1 读数变化；C3 所在小时完全未变
        rows2 = [dict(r) for r in rows1]
        rows2[0]["reading"] = 10.9
        self.publish(rows2, name="第二批")
        cur = self.store.current()
        a4 = next(r for r in cur["rows"] if r["obs_id"] == "A4")
        c3 = next(r for r in cur["rows"] if r["obs_id"] == "C3")
        self.assertFalse(a4["release"]["valid"])   # 参与对照的记录变了 => 失效
        self.assertFalse(a4["trusted"])
        self.assertTrue(c3["release"]["valid"])    # 依据未变 => 保持有效
        self.assertTrue(c3["trusted"])
        # 旧页面的放行操作提交过期依据 => 拒绝；刷新后用新依据 => 成功
        with self.assertRaises(StaleBasisError):
            self.store.release("A4", old_basis_a4, "")
        self.store.release("A4", a4["basis"], "重新放行")
        cur = self.store.current()
        self.assertTrue(next(r for r in cur["rows"] if r["obs_id"] == "A4")["trusted"])

    def test_identical_reupload_keeps_release(self):
        self.publish()
        self.store.release("A4", self.basis("A4"), "")
        self.publish(name="相同重传")
        cur = self.store.current()
        self.assertTrue(next(r for r in cur["rows"] if r["obs_id"] == "A4")["release"]["valid"])

    def test_unrelated_change_keeps_release(self):
        self.publish()
        self.store.release("A4", self.basis("A4"), "")
        rows2 = [dict(r) for r in sample_rows()]
        rows2[4]["reading"] = 55.0  # B1，与 A4 不同小时
        self.publish(rows2)
        cur = self.store.current()
        self.assertTrue(next(r for r in cur["rows"] if r["obs_id"] == "A4")["release"]["valid"])

    def test_threshold_change_invalidates_release(self):
        self.publish(threshold=5.0)
        self.store.release("A4", self.basis("A4"), "")
        self.publish(threshold=100.0)  # 阈值变化 => A4 变为正常，旧放行失效
        cur = self.store.current()
        a4 = next(r for r in cur["rows"] if r["obs_id"] == "A4")
        self.assertEqual(a4["status"], NORMAL)
        self.assertFalse(a4["release"]["valid"])

    def test_concurrent_releases(self):
        # 8 条异常（各自独立小时，参照站读数 10），多线程同时放行不同观测
        lines = []
        for i in range(8):
            h = f"2026-11-14T{10 + i:02d}:00Z"
            lines.append(f"X{i},SX{i},{h},G,900")
            lines += [f"R{i}a,SA,{h},G,10",
                      f"R{i}b,SB,{h},G,10",
                      f"R{i}c,SC,{h},G,10"]
        rows = parse_csv(csv_bytes(lines))
        self.publish(rows, threshold=1.0)
        cur = self.store.current()
        anomalies = [r for r in cur["rows"] if r["status"] == ANOMALY]
        self.assertEqual(len(anomalies), 8)
        errors = []

        def work(r):
            try:
                self.store.release(r["obs_id"], r["basis"], "并发")
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=work, args=(r,)) for r in anomalies]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        cur = self.store.current()
        self.assertEqual(cur["summary"]["released_valid"], 8)


class TestDownload(unittest.TestCase):
    def test_download_only_trusted(self):
        rows, summary = evaluate(sample_rows(), 5.0)
        payload = {"rows": rows, "summary": summary}
        # 无放行：只有 3 条正常可信
        view = build_view(payload, {})
        trusted = [r for r in view["rows"] if r["trusted"]]
        self.assertEqual(len(trusted), 3)
        text = build_download_csv(trusted)
        got = list(csv.reader(io.StringIO(text)))
        self.assertEqual(got[0], ["观测ID", "站点", "UTC小时", "分组", "读数", "状态", "放行备注"])
        self.assertEqual(len(got), 4)
        self.assertTrue(all(row[5] == "正常" for row in got[1:]))
        # 放行 A4 后：4 条可信，A4 标记「已放行」
        a4 = next(r for r in rows if r["obs_id"] == "A4")
        view = build_view(payload, {"A4": {"basis_hash": a4["basis"], "note": "确认",
                                           "updated_at": "t", "batch_id": 1}})
        trusted = [r for r in view["rows"] if r["trusted"]]
        self.assertEqual(len(trusted), 4)
        text = build_download_csv(trusted)
        got = list(csv.reader(io.StringIO(text)))
        a4_row = next(row for row in got[1:] if row[0] == "A4")
        self.assertEqual(a4_row[5], "已放行")
        self.assertEqual(a4_row[6], "确认")
        # 待补的 B1/B2 不在下载中
        self.assertNotIn("B1", [row[0] for row in got[1:]])


if __name__ == "__main__":
    unittest.main()
