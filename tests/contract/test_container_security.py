"""Static production-container security contracts."""

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ProductionContainerSecurityContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        cls.compose = (ROOT / "docker-compose.example.yml").read_text(
            encoding="utf-8"
        )

    def test_image_excludes_interactive_network_diagnostics(self):
        install_block = re.search(
            r"apt-get install(?P<body>.*?)update-ca-certificates",
            self.dockerfile,
            re.DOTALL,
        )
        self.assertIsNotNone(install_block)
        for tool in ("curl", "dnsutils", "nslookup", "dig", "iputils-ping"):
            self.assertNotIn(tool, install_block.group("body"))
        self.assertNotIn("docker-entrypoint.sh", self.dockerfile)

    def test_python_probe_and_direct_python_entrypoint_are_used(self):
        probe = '["python", "-m", "src.container_probe"]'
        compose_probe = '["CMD", "python", "-m", "src.container_probe"]'
        self.assertIn(f"CMD {probe}", self.dockerfile)
        self.assertEqual(2, self.compose.count(f"test: {compose_probe}"))
        self.assertNotIn('test: ["CMD", "curl"', self.compose)
        self.assertIn('ENTRYPOINT ["python", "-m", "src.main"]', self.dockerfile)

    def test_runtime_is_numeric_non_root_and_does_not_write_bytecode(self):
        self.assertRegex(self.dockerfile, r"(?m)^USER 1000:1000$")
        self.assertIn("PYTHONDONTWRITEBYTECODE=1", self.dockerfile)
        self.assertNotRegex(self.dockerfile, r"(?m)^VOLUME\s+/app(?:/|$)")
        self.assertRegex(self.compose, r"(?m)^\s+read_only:\s+true\b")
        self.assertRegex(self.compose, r"(?ms)^\s+cap_drop:\s*\n\s+- ALL\b")


if __name__ == "__main__":
    unittest.main()
