"""Exercise cluster orchestration without contacting SSH hosts or starting Ray."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
INFRA = HERE / "infra"


class ClusterScriptsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / "commands.jsonl"
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        recorder = bin_dir / "recorder"
        recorder.write_text(
            f"#!{sys.executable}\n"
            "import json, os, shlex, subprocess, sys\n"
            "from pathlib import Path\n"
            "tool = Path(sys.argv[0]).name\n"
            "args = sys.argv[1:]\n"
            "record = dict(tool=tool, args=args)\n"
            "if tool == 'ssh':\n"
            "    record['command'] = shlex.split(args[-1])[2]\n"
            "    if 'bash -s' in record['command']:\n"
            "        record['stdin'] = sys.stdin.read()\n"
            "with open(os.environ['COMMAND_LOG'], 'a') as stream:\n"
            "    stream.write(json.dumps(record) + '\\n')\n"
            "if os.environ.get('FAIL_MATCH') and any(os.environ['FAIL_MATCH'] in a for a in args):\n"
            "    print('simulated remote failure', file=sys.stderr)\n"
            "    sys.exit(23)\n"
            "if tool == 'ssh' and os.environ.get('EXECUTE_REMOTE'):\n"
            "    sys.exit(subprocess.run(['/bin/sh', '-c', args[-1]], input=record.get('stdin'), text=True).returncode)\n"
        )
        recorder.chmod(0o755)
        for tool in ("ssh", "rsync", "bench-python"):
            (bin_dir / tool).symlink_to(recorder)
        wheelhouse = self.root / "wheelhouse"
        wheelhouse.mkdir()
        self.pylance = wheelhouse / "pylance-12.0.0-py3-none-any.whl"
        with zipfile.ZipFile(self.pylance, "w") as wheel:
            wheel.writestr("lance/dataset.py", "def prewarm_index(partition_ids): ...")
            wheel.writestr(
                "lance/lance/__init__.pyi",
                "l1_partition_capacity_bytes index_cache_stats",
            )
        self.ray = wheelhouse / "ray-2.58.0-py3-none-any.whl"
        self.ray.touch()
        config = self.root / "cluster.env"
        config.write_text(
            "SSH_USER=ubuntu\nCOORD_IP=10.42.0.10\nACTOR0_IP=10.42.0.11\n"
            "ACTOR1_IP=10.42.0.12\nMINIO_HOST=10.42.0.20\n"
            "COORD_SSH_HOST=head-alias\n"
            f"WHEELHOUSE={shlex.quote(str(wheelhouse))}\n"
            "REMOTE_BENCH_DIR='/home/ubuntu/remote bench'\n"
            "NVME_DIR='/mnt/local disk/lance'\n"
        )
        self.environment = {
            **os.environ,
            "CLUSTER_ENV": str(config),
            "COMMAND_LOG": str(self.log),
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "OUT_DIR": str(self.root / "comparison"),
            "BENCH_PYTHON": str(bin_dir / "bench-python"),
            "LOCAL_LOG_DIR": str(self.root / "client logs"),
        }

    def run_script(self, script, *arguments, **environment):
        return subprocess.run(
            ["bash", str(script), *arguments],
            env={**self.environment, **environment},
            capture_output=True,
            text=True,
            timeout=20,
        )

    def commands(self, tool):
        if not self.log.exists():
            return []
        return [
            record
            for line in self.log.read_text().splitlines()
            if (record := json.loads(line))["tool"] == tool
        ]

    def test_deploy_excludes_state_and_reinstalls_same_version_wheels(self):
        result = self.run_script(INFRA / "ship_real_cluster.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        source_syncs = [
            call["args"]
            for call in self.commands("rsync")
            if call["args"][-2] == f"{HERE}/"
        ]
        self.assertEqual(len(source_syncs), 3)
        for arguments in source_syncs:
            for exclusion in (
                "/.venv/",
                "/out/",
                "/results/",
                "/infra/real_cluster.env",
            ):
                self.assertIn(exclusion, arguments)
        installs = [call["stdin"] for call in self.commands("ssh") if "stdin" in call]
        self.assertEqual(len(installs), 3)
        for install in installs:
            self.assertIn("--force-reinstall", install)
            self.assertIn("--no-index", install)
            self.assertIn("_validate_lance_api", install)

    def test_code_only_deploy_does_not_install_or_ship_wheels(self):
        self.ray.unlink()
        result = self.run_script(INFRA / "ship_real_cluster.sh", "--code-only")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.commands("rsync")), 4)
        self.assertFalse(any("stdin" in call for call in self.commands("ssh")))

    def test_start_assigns_one_actor_slot_to_each_worker(self):
        result = self.run_script(INFRA / "run_real_cluster.sh", "start")
        self.assertEqual(result.returncode, 0, result.stderr)
        starts = [
            shlex.split(call["command"])
            for call in self.commands("ssh")
            if "/bin/ray start" in call["command"]
        ]
        self.assertEqual(len(starts), 3)
        self.assertIn("--num-cpus=0", starts[0])
        for index, command in enumerate(starts[1:], start=11):
            self.assertIn(f"--node-ip-address=10.42.0.{index}", command)
            self.assertIn('--resources={"search_actor_node":1}', command)

    def test_smoke_passes_routable_endpoint_and_quotes_extra_arguments(self):
        dataset = "s3://bucket/path with spaces/'quoted'/$literal;$(touch unwanted)"
        result = self.run_script(
            INFRA / "run_real_cluster.sh", "smoke", "--dataset-uri", dataset
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.commands("ssh")
        self.assertEqual(len(calls), 2)
        self.assertIn("ubuntu@head-alias", calls[-1]["args"])
        command = shlex.split(calls[-1]["command"])
        for flag, expected in (
            ("--num-actors", "2"),
            ("--actor-resource", "search_actor_node"),
            ("--endpoint-url", "http://10.42.0.20:9000"),
            ("--nvme-dir", "/mnt/local disk/lance"),
            ("--dataset-uri", dataset),
        ):
            self.assertEqual(command[command.index(flag) + 1], expected)
        self.assertIn("--invalidation-drill", command)

    def test_remote_arguments_survive_a_posix_login_shell(self):
        argument = "line one\n'quoted' $(exit 99); $literal"
        code = "import json, sys\nprint(json.dumps(sys.argv[1:]))"
        result = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; SSH_USER=ubuntu; remote local "$2" -c "$3" "$4"',
                "test-remote",
                str(INFRA / "real_cluster_common.sh"),
                sys.executable,
                code,
                argument,
            ],
            env={**self.environment, "EXECUTE_REMOTE": "1"},
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), [argument])

    def test_worker_check_failure_prevents_benchmark_submission(self):
        result = self.run_script(
            INFRA / "run_real_cluster.sh", "run", FAIL_MATCH="ray.init"
        )
        self.assertEqual(result.returncode, 23)
        self.assertEqual(len(self.commands("ssh")), 1)
        logs = list(Path(self.environment["LOCAL_LOG_DIR"]).glob("client-run-*.log"))
        self.assertEqual(len(logs), 1)
        self.assertIn("simulated remote failure", logs[0].read_text())

    def test_run_logs_stdout_stderr_and_failure_status_on_client_and_head(self):
        head = self.root / "head bench"
        python = head / ".venv/bin/python"
        python.parent.mkdir(parents=True)
        python.write_text(
            f"#!{sys.executable}\n"
            "import os, sys\n"
            "if sys.argv[1] == '-c':\n"
            "    print('precheck stdout', flush=True)\n"
            "    print('precheck stderr', file=sys.stderr, flush=True)\n"
            "    sys.exit(0)\n"
            "print('benchmark stdout', flush=True)\n"
            "print('benchmark stderr', file=sys.stderr, flush=True)\n"
            "sys.exit(int(os.environ['BENCHMARK_EXIT_CODE']))\n"
        )
        python.chmod(0o755)
        with Path(self.environment["CLUSTER_ENV"]).open("a") as config:
            config.write(f"REMOTE_BENCH_DIR={shlex.quote(str(head))}\n")

        client_logs = set()
        head_logs = set()
        for exit_code in (0, 23):
            with self.subTest(exit_code=exit_code):
                result = self.run_script(
                    INFRA / "run_real_cluster.sh",
                    "run",
                    EXECUTE_REMOTE="1",
                    BENCHMARK_EXIT_CODE=str(exit_code),
                )
                self.assertEqual(result.returncode, exit_code, result.stderr)
                current_client = set(
                    Path(self.environment["LOCAL_LOG_DIR"]).glob("client-run-*.log")
                )
                current_head = set((head / "out/logs").glob("run-*.log"))
                self.assertEqual(len(current_client - client_logs), 1)
                self.assertEqual(len(current_head - head_logs), 1)
                client_text = next(iter(current_client - client_logs)).read_text()
                head_text = next(iter(current_head - head_logs)).read_text()
                for message in ("benchmark stdout", "benchmark stderr"):
                    self.assertIn(message, result.stdout)
                    self.assertIn(message, client_text)
                    self.assertIn(message, head_text)
                for message in ("precheck stdout", "precheck stderr"):
                    self.assertIn(message, result.stdout)
                    self.assertIn(message, client_text)
                self.assertIn(f"exit status: {exit_code}", head_text)
                client_logs, head_logs = current_client, current_head

    def test_stop_attempts_remaining_nodes_after_an_ssh_failure(self):
        result = self.run_script(
            INFRA / "run_real_cluster.sh", "stop", FAIL_MATCH="@10.42.0.11"
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.commands("ssh")), 3)

    def test_corrupt_artifact_is_rejected_before_any_ssh(self):
        checksum = hashlib.sha256(self.ray.read_bytes()).hexdigest()
        (self.ray.parent / "SHA256SUMS").write_text(f"{checksum}  {self.ray.name}\n")
        self.ray.write_bytes(b"changed wheel")
        result = self.run_script(INFRA / "ship_real_cluster.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.commands("ssh"), [])

    def test_standalone_wheel_check_does_not_require_ray(self):
        self.ray.unlink()
        result = self.run_script(
            INFRA / "verify_wheelhouse.sh", "--lance-wheel", str(self.pylance)
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_editable_wheel_rejected_before_dependency_downloads(self):
        with zipfile.ZipFile(self.pylance, "w") as wheel:
            wheel.writestr("pylance.pth", "/build-machine/lance/python/python")
        result = self.run_script(
            INFRA / "build_wheelhouse.sh",
            LANCE_WHEEL=str(self.pylance),
            BUILD_PYTHON=self.environment["BENCH_PYTHON"],
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Editable pylance wheel cannot be deployed", result.stderr)
        self.assertIn("maturin build", result.stderr)
        self.assertEqual(self.commands("bench-python"), [])
        self.assertEqual(list(self.pylance.parent.glob(".staging.*")), [])

    def test_editable_wheel_is_rejected_before_shipping(self):
        with zipfile.ZipFile(self.pylance, "w") as wheel:
            wheel.writestr("pylance.pth", "/build-machine/lance/python/python")
        result = self.run_script(INFRA / "ship_real_cluster.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Editable pylance wheel cannot be deployed", result.stderr)
        self.assertEqual(self.commands("ssh"), [])

    def test_failed_download_retains_staging_and_previous_wheels(self):
        previous_pylance = self.pylance.read_bytes()
        result = self.run_script(
            INFRA / "build_wheelhouse.sh",
            LANCE_WHEEL=str(self.pylance),
            BUILD_PYTHON=self.environment["BENCH_PYTHON"],
            FAIL_MATCH="download",
        )
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIn("downloaded artifacts retained at", result.stderr)
        self.assertEqual(len(list(self.pylance.parent.glob(".staging.*"))), 1)
        self.assertEqual(self.pylance.read_bytes(), previous_pylance)
        self.assertTrue(self.ray.exists())

    def test_missing_wheelhouse_explains_build_must_succeed_first(self):
        self.ray.unlink()
        result = self.run_script(INFRA / "ship_real_cluster.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("build_wheelhouse.sh successfully before shipping", result.stderr)
        self.assertEqual(self.commands("ssh"), [])

    def test_verifier_accepts_relocated_python_modules(self):
        for library in ("purelib", "platlib"):
            with self.subTest(library=library):
                prefix = f"pylance-12.0.0.data/{library}/"
                with zipfile.ZipFile(self.pylance, "w") as wheel:
                    wheel.writestr(
                        prefix + "lance/dataset.py",
                        "def prewarm_index(partition_ids): ...",
                    )
                    wheel.writestr(
                        prefix + "lance/lance/__init__.pyi",
                        "l1_partition_capacity_bytes index_cache_stats",
                    )
                result = self.run_script(
                    INFRA / "verify_wheelhouse.sh", str(self.pylance.parent)
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_verifier_missing_module_identifies_the_wheel(self):
        with zipfile.ZipFile(self.pylance, "w") as wheel:
            wheel.writestr("unrelated/dataset.py", "partition_ids")
        result = self.run_script(
            INFRA / "verify_wheelhouse.sh", str(self.pylance.parent)
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("lance/dataset.py", result.stderr)
        self.assertIn(str(self.pylance), result.stderr)
        self.assertIn("LANCE_WHEEL", result.stderr)
        self.assertNotIn("caution: filename not matched", result.stderr)

    def test_verifier_rejects_wheel_without_cache_counters(self):
        with zipfile.ZipFile(self.pylance, "w") as wheel:
            wheel.writestr("lance/dataset.py", "partition_ids")
            wheel.writestr("lance/lance/__init__.pyi", "l1_partition_capacity_bytes")
        result = self.run_script(
            INFRA / "verify_wheelhouse.sh", "--lance-wheel", str(self.pylance)
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing Session.index_cache_stats", result.stderr)
        self.assertIn("Rebuild Lance", result.stderr)

    def test_verifier_rejects_ambiguous_module_paths(self):
        with zipfile.ZipFile(self.pylance, "a") as wheel:
            wheel.writestr(
                "pylance-12.0.0.data/purelib/lance/dataset.py", "partition_ids"
            )
        result = self.run_script(
            INFRA / "verify_wheelhouse.sh", str(self.pylance.parent)
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("found 2", result.stderr)

    def test_verifier_still_rejects_relocated_legacy_api(self):
        with zipfile.ZipFile(self.pylance, "w") as wheel:
            wheel.writestr(
                "pylance-12.0.0.data/purelib/lance/dataset.py", "partition_ids"
            )
            wheel.writestr(
                "pylance-12.0.0.data/platlib/lance/lance/__init__.pyi",
                "moka_l1_partition_bytes",
            )
        result = self.run_script(
            INFRA / "verify_wheelhouse.sh", str(self.pylance.parent)
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing the Lance 12 two-level cache API", result.stderr)

    def test_comparison_uses_cluster_placement_for_every_profile(self):
        result = self.run_script(
            HERE / "run_comparison.sh",
            "--drop-l2-page-cache-on-restart",
            REPETITIONS="1",
            NUM_ACTORS="2",
            ACTOR_RESOURCE="search_actor_node",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.commands("bench-python")
        self.assertEqual(len(calls), 3)
        for call in calls:
            command = call["args"]
            self.assertEqual(command[command.index("--num-actors") + 1], "2")
            self.assertEqual(
                command[command.index("--actor-resource") + 1], "search_actor_node"
            )
            self.assertIn("--drop-l2-page-cache-on-restart", command)

    def test_comparison_failure_shows_remote_log(self):
        result = self.run_script(
            HERE / "run_comparison.sh", REPETITIONS="1", FAIL_MATCH="run_benchmark.py"
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("simulated remote failure", result.stderr)
        self.assertEqual(len(self.commands("bench-python")), 1)


if __name__ == "__main__":
    unittest.main()
