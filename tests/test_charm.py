# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import unittest
from unittest import mock

from ops import model, testing

import charm


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = testing.Harness(charm.WaltzOperatorCharm)
        self.addCleanup(self.harness.cleanup)

    def _patch(self, obj, method):
        """Patches the given method and returns its Mock."""
        patcher = mock.patch.object(obj, method)
        mock_patched = patcher.start()
        self.addCleanup(patcher.stop)

        return mock_patched

    @mock.patch("psycopg2.connect")
    def test_waltz_pebble_ready(self, mock_connect):
        # Check the initial Pebble plan is empty
        initial_plan = self.harness.get_container_pebble_plan("waltz")
        self.assertEqual(initial_plan.to_yaml(), "{}\n")

        # Get the waltz container from the model and emit the PebbleReadyEvent carrying it.
        self.harness.begin_with_initial_hooks()
        container = self.harness.model.unit.get_container("waltz")
        self.harness.charm.on.waltz_pebble_ready.emit(container)

        # No datebase host was configured, so the status should be Waiting.
        self.assertIsInstance(self.harness.model.unit.status, model.BlockedStatus)

        # Update the charm config, and emit the PebbleReadyEvent again, but we can't
        # connect to the container yet.
        mock_can_connect = self._patch(container, "can_connect")
        mock_can_connect.return_value = False
        self.harness.charm.on.waltz_pebble_ready.emit(container)
        self.harness.update_config({"db-host": "foo.lish"})
        self.assertIsInstance(self.harness.model.unit.status, model.WaitingStatus)

        # Reemit the PebbleReadyEvent, and the container can be connected to. The charm
        # should become Active.
        mock_can_connect.return_value = True
        self.harness.charm.on.waltz_pebble_ready.emit(container)
        self.assertEqual(self.harness.model.unit.status, model.ActiveStatus())

        # Check the service was started
        service = self.harness.model.unit.get_container("waltz").get_service("waltz")
        self.assertTrue(service.is_running())

        # Get the plan now we've run PebbleReady and check that we've got the plan we expected.
        updated_plan = self.harness.get_container_pebble_plan("waltz").to_dict()
        expected_plan = {
            "services": {
                "waltz": {
                    "override": "replace",
                    "summary": "waltz",
                    "command": "/bin/sh -c 'docker-entrypoint.sh update run'",
                    "startup": "enabled",
                    "user": "waltz",
                    "environment": {
                        "DB_HOST": self.harness.charm.config["db-host"],
                        "DB_PORT": self.harness.charm.config["db-port"],
                        "DB_NAME": self.harness.charm.config["db-name"],
                        "DB_USER": self.harness.charm.config["db-username"],
                        "DB_PASSWORD": self.harness.charm.config["db-password"],
                        "DB_SCHEME": "waltz",
                        "WALTZ_FROM_EMAIL": "help@finos.org",
                        "CHANGELOG_FILE": "/opt/waltz/liquibase/db.changelog-master.xml",
                    },
                }
            },
        }
        self.assertDictEqual(expected_plan, updated_plan)

        # Emit the Pebble ready again, make sure that the container is NOT restarted if
        # there was no configuration change.
        mock_restart = self._patch(container, "restart")
        self.harness.charm.on.waltz_pebble_ready.emit(container)
        mock_restart.assert_not_called()

    @mock.patch("psycopg2.connect")
    def test_config_changed(self, mock_connect):
        self.harness.begin_with_initial_hooks()
        self.assertIsInstance(self.harness.model.unit.status, model.BlockedStatus)

        # Update the port, expect it to remain in WaitingStatus.
        self.harness.update_config({"db-port": 9999})
        self.assertIsInstance(self.harness.model.unit.status, model.BlockedStatus)

        # Update the host but fail to connect to postgresql. The status should be Blocked.
        mock_connect.side_effect = Exception("expected exception.")
        expected_host = "reach-you-cannot"
        self.harness.update_config({"db-host": expected_host})

        self.assertIsInstance(self.harness.model.unit.status, model.BlockedStatus)
        mock_connect.assert_called_once_with(
            host=expected_host,
            port=9999,
            dbname=self.harness.charm.config["db-name"],
            user=self.harness.charm.config["db-username"],
            password=self.harness.charm.config["db-password"],
        )

        # Update the host again, expect it to become Active.
        mock_connect.side_effect = None
        expected_host = "foo.lish"
        self.harness.update_config({"db-host": expected_host})

        self.assertEqual(self.harness.model.unit.status, model.ActiveStatus())
        mock_connect.assert_called_with(
            host=expected_host,
            port=9999,
            dbname=self.harness.charm.config["db-name"],
            user=self.harness.charm.config["db-username"],
            password=self.harness.charm.config["db-password"],
        )
        mock_connect.return_value.close.assert_called()
