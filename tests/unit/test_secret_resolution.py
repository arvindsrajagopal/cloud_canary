"""Regressions for sanitized inline and file-backed secret resolution."""

import configparser
import traceback
import unittest
from unittest.mock import mock_open, patch

from src.config import load_config, resolve_secret_sources


def _config():
    return {
        "kafka": {
            "sasl.password": "kafka-inline",
        },
        "schema_registry": {
            "basic.auth.user.info": "sr-user:sr-inline",
        },
        "app": {},
    }


class SecretResolutionTests(unittest.TestCase):
    def test_load_config_resolves_files_before_validation(self):
        parser = configparser.ConfigParser()
        parser.read_dict(
            {
                "kafka": {
                    "bootstrap.servers": "broker.invalid:9092",
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": "user",
                    "sasl.password.file": "/configured/kafka-secret",
                },
                "schema_registry": {
                    "url": "https://registry.invalid",
                    "basic.auth.user.info": "user:inline-secret",
                },
                "app": {"topic": "canary"},
            }
        )

        with (
            patch("src.config.os.path.exists", return_value=True),
            patch("src.config.configparser.ConfigParser", return_value=parser),
            patch(
                "src.config._read_secret_file", return_value="resolved-secret\n"
            ),
        ):
            config = load_config("config/config.ini")

        self.assertEqual("resolved-secret", config["kafka"]["sasl.password"])
        self.assertNotIn("sasl.password.file", config["kafka"])

    def test_accepts_each_supported_file_source_individually(self):
        cases = (
            ("kafka", "sasl.password", "kafka-file-secret"),
            (
                "schema_registry",
                "basic.auth.user.info",
                "sr-user:sr-file-secret",
            ),
            ("kafka", "ssl.key.password", "key-file-secret"),
        )
        for section, setting, secret in cases:
            with self.subTest(setting=setting):
                config = _config()
                config[section].pop(setting, None)
                config[section][f"{setting}.file"] = "/mounted/secret"
                with patch(
                    "src.config._read_secret_file", return_value=secret
                ) as read_secret:
                    resolve_secret_sources(config)

                read_secret.assert_called_once_with("/mounted/secret")
                self.assertEqual(secret, config[section][setting])
                self.assertNotIn(f"{setting}.file", config[section])

    def test_accepts_inline_sources_and_optional_secret_omission(self):
        config = _config()

        resolve_secret_sources(config)

        self.assertEqual("kafka-inline", config["kafka"]["sasl.password"])
        self.assertNotIn("ssl.key.password", config["kafka"])

    def test_rejects_dual_sources_without_disclosing_values(self):
        config = _config()
        secret = "must-not-escape"
        config["kafka"]["sasl.password"] = secret
        config["kafka"]["sasl.password.file"] = "/mounted/secret"

        with self.assertRaises(ValueError) as raised:
            resolve_secret_sources(config)

        message = str(raised.exception)
        self.assertIn("[kafka].sasl.password", message)
        self.assertNotIn(secret, message)
        self.assertNotIn("/mounted/secret", message)

    def test_rejects_each_missing_required_source(self):
        for section, setting in (
            ("kafka", "sasl.password"),
            ("schema_registry", "basic.auth.user.info"),
        ):
            with self.subTest(setting=setting):
                config = _config()
                del config[section][setting]

                with self.assertRaisesRegex(
                    ValueError, rf"\[{section}\]\.{setting}"
                ):
                    resolve_secret_sources(config)

    def test_removes_only_one_conventional_trailing_line_ending(self):
        for raw, expected in (
            ("secret\n", "secret"),
            ("secret\r", "secret"),
            ("secret\r\n", "secret"),
            ("secret\n\n", "secret\n"),
            ("secret\r\n\r\n", "secret\r\n"),
            ("secret", "secret"),
        ):
            with self.subTest(raw=repr(raw)):
                config = _config()
                del config["kafka"]["sasl.password"]
                config["kafka"]["sasl.password.file"] = "/mounted/secret"
                with patch("src.config._read_secret_file", return_value=raw):
                    resolve_secret_sources(config)
                self.assertEqual(expected, config["kafka"]["sasl.password"])

    def test_rejects_empty_resolved_value_without_disclosure(self):
        for raw in ("", "\n", "\r", "\r\n"):
            with self.subTest(raw=repr(raw)):
                config = _config()
                del config["kafka"]["sasl.password"]
                config["kafka"]["sasl.password.file"] = "/mounted/secret"
                with (
                    patch("src.config._read_secret_file", return_value=raw),
                    self.assertRaises(ValueError) as raised,
                ):
                    resolve_secret_sources(config)

                self.assertIn("[kafka].sasl.password", str(raised.exception))
                self.assertNotIn("/mounted/secret", str(raised.exception))

    def test_read_errors_identify_setting_without_sensitive_details(self):
        config = _config()
        del config["schema_registry"]["basic.auth.user.info"]
        config["schema_registry"]["basic.auth.user.info.file"] = (
            "/sensitive/location"
        )
        with (
            patch(
                "src.config._read_secret_file",
                side_effect=OSError("raw-secret and sensitive path"),
            ),
            self.assertRaises(ValueError) as raised,
        ):
            resolve_secret_sources(config)

        message = str(raised.exception)
        self.assertIn("[schema_registry].basic.auth.user.info", message)
        self.assertNotIn("raw-secret", message)
        self.assertNotIn("/sensitive/location", message)
        rendered = "".join(
            traceback.format_exception(
                type(raised.exception), raised.exception, raised.exception.__traceback__
            )
        )
        self.assertNotIn("raw-secret", rendered)
        self.assertNotIn("/sensitive/location", rendered)

    def test_reader_uses_configured_path_and_preserves_line_endings(self):
        opened = mock_open(read_data="secret\r\n")
        config = _config()
        del config["kafka"]["sasl.password"]
        config["kafka"]["sasl.password.file"] = "/configured/path"

        with patch("builtins.open", opened):
            resolve_secret_sources(config)

        opened.assert_called_once_with(
            "/configured/path", encoding="utf-8", newline=""
        )


if __name__ == "__main__":
    unittest.main()
