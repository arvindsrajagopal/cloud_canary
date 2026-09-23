"""Contract regressions for private-by-default endpoint exposure."""

import inspect
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from src import metrics
import src.main as main


class EndpointExposureTests(TestCase):
    def test_all_application_defaults_bind_to_loopback(self):
        self.assertEqual(
            "127.0.0.1",
            inspect.signature(metrics.start_metrics_server)
            .parameters["addr"]
            .default,
        )

        template = (
            Path(__file__).parents[2] / "config" / "config.ini.template"
        ).read_text(encoding="utf-8")
        self.assertIn("metrics.bind.address=127.0.0.1", template)
        self.assertNotIn("metrics.bind.address=0.0.0.0", template)

        config = {"kafka": {}, "schema_registry": {}, "app": {}}
        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.configure_health_state"),
            patch("src.main._run_startup_dependency"),
            patch(
                "src.main.start_metrics_server",
                side_effect=RuntimeError("stop after HTTP startup"),
            ) as start_server,
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after HTTP startup"):
                main._run_lifecycle()

        self.assertEqual("127.0.0.1", start_server.call_args.kwargs["addr"])

    def test_non_loopback_plaintext_bind_emits_sanitized_security_warning(self):
        with (
            patch.object(metrics.log, "warning") as warning,
            patch(
                "http.server.HTTPServer",
                side_effect=RuntimeError("stop after exposure check"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after exposure check"):
                metrics.start_metrics_server(8000, addr="0.0.0.0")

        warning.assert_called_once()
        message = warning.call_args.args[0]
        self.assertIn("SECURITY WARNING", message)
        self.assertIn("plaintext HTTP", message)
        self.assertIn("access-restricted private network", message)
        self.assertIn("firewall", message)
        self.assertIn("network policy", message)
        self.assertIn("reverse proxy", message)
        self.assertIn("service mesh", message)
        self.assertIn("does not provide authentication or authorization", message)
        self.assertNotIn("0.0.0.0", message)

    def test_loopback_and_tls_listeners_do_not_emit_exposure_warning(self):
        with (
            patch.object(metrics.log, "warning") as warning,
            patch(
                "http.server.HTTPServer",
                side_effect=RuntimeError("stop after exposure check"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after exposure check"):
                metrics.start_metrics_server(8000, addr="127.0.0.1")

        warning.assert_not_called()

        with patch.object(metrics.log, "warning") as warning:
            with self.assertRaisesRegex(ValueError, "must be set"):
                metrics.start_metrics_server(
                    8000,
                    addr="0.0.0.0",
                    ssl_enabled=True,
                )

        warning.assert_not_called()


if __name__ == "__main__":
    import unittest

    unittest.main()
