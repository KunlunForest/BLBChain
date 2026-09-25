import csv
import json
from pathlib import Path
import tempfile
import unittest

from run_overhead import prepare_dataset
from summarize_overhead import percentile, summarize


class ExperimentToolsTest(unittest.TestCase):
    def test_normalization_preserves_integer_amount(self):
        with tempfile.TemporaryDirectory() as d:
            source,target=Path(d)/"source.csv",Path(d)/"target.csv"
            amount="123456789012345678901234567890123456"
            source.write_text("i,x,s,r,v\n0,,0x"+"A"*40+",0x"+"B"*40+","+amount+"\n")
            prepare_dataset(source,target,1,2,3,4)
            with target.open() as f:
                rows=list(csv.DictReader(f))
            self.assertEqual(rows[0]["sender"],"a"*40)
            self.assertEqual(rows[0]["value"],amount)
            with self.assertRaises(ValueError):
                prepare_dataset(source,target,2,2,3,4)

    def test_failed_runs_cannot_leave_stale_summary(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);trial=root/"A_bw1_r1";trial.mkdir()
            (trial/"status.json").write_text(json.dumps(dict(status="failed",error="timeout")))
            (root/"summary.csv").write_text("old,fake\n1,2\n")
            self.assertFalse(summarize(root))
            with (root/"summary.csv").open() as f:
                self.assertEqual(list(csv.DictReader(f)),[])
            self.assertIn("timeout",(root/"invalid_trials.csv").read_text())

    def test_nearest_rank_p95(self):
        self.assertEqual(percentile(list(range(1,101)),.95),95)
        self.assertEqual(percentile([],.95),"")


if __name__=="__main__":unittest.main()
