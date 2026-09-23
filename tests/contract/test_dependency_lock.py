"""Production dependency hash-lock contracts."""

import hashlib
import io
import re
import unittest
from pathlib import Path

from pip._internal.exceptions import HashMismatch
from pip._internal.utils.hashes import Hashes


ROOT = Path(__file__).resolve().parents[2]

EXPECTED_PACKAGES = {
    "anyio",
    "attrs",
    "authlib",
    "avro",
    "cachetools",
    "certifi",
    "cffi",
    "charset-normalizer",
    "confluent-kafka",
    "cryptography",
    "fastavro",
    "h11",
    "httpcore",
    "httpx",
    "idna",
    "joserfc",
    "prometheus-client",
    "pycparser",
    "requests",
    "typing-extensions",
    "urllib3",
}


class ProductionDependencyLockContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lock = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        cls.dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    def test_every_resolved_requirement_is_exactly_pinned_and_hashed(self):
        logical_lines = re.sub(r"\\\n\s*", " ", self.lock).splitlines()
        requirements = [
            line for line in logical_lines
            if line and not line.startswith(("#", "--"))
        ]
        package_names = {
            re.match(r"^([A-Za-z0-9_.-]+)", requirement).group(1).lower()
            for requirement in requirements
        }
        self.assertEqual(EXPECTED_PACKAGES, package_names)
        for requirement in requirements:
            self.assertRegex(requirement, r"^[A-Za-z0-9_.-]+(?:\[[^]]+\])?==[^ ]+")
            hashes = re.findall(r"--hash=sha256:([0-9a-f]{64})(?:\s|$)", requirement)
            self.assertTrue(hashes, requirement)
        self.assertIn("fastavro==", self.lock)
        self.assertRegex(self.lock, r"(?m)^--only-binary(?:=|\s+):all:$")

    def test_production_install_enforces_hash_verification(self):
        install = re.search(r"RUN pip install (?P<args>[^\n]+)", self.dockerfile)
        self.assertIsNotNone(install)
        self.assertIn("--require-hashes", install.group("args"))
        self.assertIn("-r requirements.txt", install.group("args"))

    def test_pip_hash_verifier_rejects_an_altered_artifact(self):
        original = b"approved distribution artifact"
        approved = hashlib.sha256(original).hexdigest()
        verifier = Hashes({"sha256": [approved]})

        verifier.check_against_file(io.BytesIO(original))
        with self.assertRaises(HashMismatch):
            verifier.check_against_file(io.BytesIO(original + b" altered"))


if __name__ == "__main__":
    unittest.main()
