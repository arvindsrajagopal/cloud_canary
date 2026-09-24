"""Offline contracts for release provenance and container startup policy."""

import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _application_version() -> str:
    tree = ast.parse((ROOT / "src" / "__version__.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("src.__version__ does not define __version__")


class SupplyChainContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        cls.entrypoint = (ROOT / "docker-entrypoint.sh").read_text(encoding="utf-8")
        cls.readme = (ROOT / "README.md").read_text(encoding="utf-8")

    def test_dependency_lock_is_not_generated_or_refreshed_by_image_build(self):
        self.assertNotRegex(
            self.dockerfile.lower(), r"pip-compile|pipenv lock|poetry lock|uv lock"
        )
        self.assertIn("pip install --no-cache-dir --require-hashes", self.dockerfile)

    def test_required_oci_provenance_has_no_placeholders(self):
        for argument in ("VERSION", "REVISION", "CREATED"):
            self.assertRegex(self.dockerfile, rf"(?m)^ARG {argument}$")
            self.assertNotRegex(self.dockerfile, rf"(?m)^ARG {argument}=")

        expected = {
            "source": "https://github.com/arvindsrajagopal/cloud_canary",
            "version": "${VERSION}",
            "revision": "${REVISION}",
            "created": "${CREATED}",
        }
        for name, value in expected.items():
            self.assertIn(
                f'org.opencontainers.image.{name}="{value}"', self.dockerfile
            )
        self.assertNotRegex(self.dockerfile.lower(), r"yourusername|xxxxx|placeholder")

    def test_image_version_is_derived_from_and_checked_against_application(self):
        version = _application_version()
        self.assertIn("from src.__version__ import __version__", self.dockerfile)
        self.assertIn("version == __version__", self.dockerfile)
        self.assertIn("from src.__version__ import __version__", self.readme)
        self.assertIn(f'cloud-canary:{version}', self.readme)

    def test_startup_is_direct_exec_and_has_no_diagnostics(self):
        self.assertIn('ENTRYPOINT ["python", "-m", "src.main"]', self.dockerfile)
        executable_lines = [
            line.strip()
            for line in self.entrypoint.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertEqual(["set -eu", 'exec "$@"'], executable_lines)
        prohibited = ("nslookup", "dig ", "ping ", "resolv.conf", "ip addr", "ifconfig")
        for token in prohibited:
            self.assertNotIn(token, self.entrypoint.lower())

    def test_architecture_claims_require_end_to_end_release_validation(self):
        readme_words = " ".join(self.readme.split())
        self.assertIn(
            "No multi-architecture image support is currently claimed", readme_words
        )
        for requirement in (
            "complete build",
            "startup",
            "native dependency import",
            "configured liveness probe",
        ):
            self.assertIn(requirement, readme_words)
        self.assertIn('importlib.import_module(name)', self.dockerfile)
        self.assertIn('"confluent_kafka", "fastavro", "cryptography"', self.dockerfile)

    def test_release_interface_supports_scanning_and_build_attestations(self):
        self.assertIn(
            "trivy image --exit-code 1 --severity HIGH,CRITICAL", self.readme
        )
        self.assertIn("--sbom=true", self.readme)
        self.assertIn("--provenance=mode=max", self.readme)
        self.assertIn("external vulnerability", self.readme)
        self.assertIn("does not embed a signing service", self.readme)


if __name__ == "__main__":
    unittest.main()
