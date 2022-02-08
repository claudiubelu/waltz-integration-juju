#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Module defining the Charmed operator for FINOS Waltz."""

import logging

from ops import charm, framework, lib, main, model, pebble

logger = logging.getLogger(__name__)

pgsql = lib.use("pgsql", 1, "postgresql-charmers@lists.launchpad.net")


class WaltzOperatorCharm(charm.CharmBase):
    """A Juju Charm to deploy FINOS Waltz on Kubernetes.

    This charm can be configured with postgresql database connection details which
    will be used by FINOS Waltz.
    """

    _state = framework.StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._init_state()

        # PostgreSQL relation hooks:
        self.db = pgsql.PostgreSQLClient(self, "db")
        self.framework.observe(
            self.db.on.database_relation_joined, self._on_database_relation_joined)
        self.framework.observe(
            self.db.on.database_relation_broken, self._on_database_relation_broken)
        self.framework.observe(self.db.on.master_changed, self._on_master_changed)

        # General hooks:
        self.framework.observe(self.on.waltz_pebble_ready, self._on_waltz_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

    def _init_state(self):
        self._state.set_default(db_host=None)
        self._state.set_default(db_port=None)
        self._state.set_default(db_name=None)
        self._state.set_default(db_username=None)
        self._state.set_default(db_password=None)

    def _on_database_relation_joined(self, event: pgsql.DatabaseRelationJoinedEvent):
        # Request a database with the configured db-name from the joined PostgreSQL server charm.
        event.database = self.config["db-name"]
        # Request the citext extension to be installed.
        # event.extensions = ["citext"]

    def _on_database_relation_broken(self, event: pgsql.DatabaseRelationBrokenEvent):
        # The relation with the PostgreSQL server charm has been broken. We need to disconnect
        # Waltz from it, and reconfigure it to use the configured database instead, if any.
        self._state.db_conn_str = None
        self._state.db_uri = None
        self._rebuild_waltz_pebble_layer(event)

    def _on_master_changed(self, event: pgsql.MasterChangedEvent):
        # Check if the requested database has been provided to us.
        if event.database != self.config["db-name"]:
            return

        if event.master is None:
            self._state.db_host = None
            self._state.db_port = None
            self._state.db_name = None
            self._state.db_username = None
            self._state.db_password = None
        else:
            self._state.db_host = event.master.host
            self._state.db_port = event.master.port
            self._state.db_name = event.master.dbname
            self._state.db_username = event.master.user
            self._state.db_password = event.master.password

        # Update Waltz with the new database connection details.
        self._rebuild_waltz_pebble_layer(event)

    def _on_waltz_pebble_ready(self, event):
        """Handles the Pebble Ready event."""
        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        self._rebuild_waltz_pebble_layer(event, container)

    def _on_config_changed(self, event):
        """Refreshes the service config.

        A new Waltz pebble layer specification will be set if any of the
        configuration options related to Waltz are updated.
        """
        self._rebuild_waltz_pebble_layer(event)

    def _rebuild_waltz_pebble_layer(self, event, container=None):
        """(Re)Builds the Waltz Pebble Layer.

        The Pebble layer will be (re)created if the credentials required by
        Waltz to connect to a postgresql database are present. The layer will
        only be recreated if it is different from the current specification.
        """
        # Check if we have all the necessary information to start Waltz.
        db_config = self._get_database_config()
        if not db_config:
            self.unit.status = model.BlockedStatus("Waiting for database configuration.")
            return

        # If not provided with a container, get one.
        container = container or self.unit.get_container("waltz")

        if not container.can_connect():
            self.unit.status = model.WaitingStatus("Waiting for Pebble to be ready")
            event.defer()
            return

        # Create the current Pebble layer configuration
        pebble_layer = pebble.Layer(self._generate_workload_pebble_layer(db_config))

        plan = container.get_plan()
        updated_services = plan.services != pebble_layer.services
        if updated_services:
            logger.info("Waltz needs to be updated. Restarting.")
            container.add_layer("waltz", pebble_layer, combine=True)
            container.restart("waltz")

        self.unit.status = model.ActiveStatus()

    def _get_database_config(self) -> dict:
        """Returns the postgresql database connection details.

        Returns the configured database connection details as a dictionary. If they are
        missing, an empty dictionary is returned instead.
        """
        # Check if we're related to a PostgreSQL Server charm. If so, return the connection
        # details to it.
        if self._state.db_host:
            return {
                "host": self._state.db_host,
                "port": self._state.db_port,
                "dbname": self._state.db_name,
                "username": self._state.db_username,
                "password": self._state.db_password,
            }

        # We're not related to a PostgreSQL Server charm. Use the existing configuration.
        host = self.config["db-host"]
        port = self.config["db-port"]
        dbname = self.config["db-name"]
        username = self.config["db-username"]
        password = self.config["db-password"]

        # If we don't have any of these configurations, we can't start Waltz.
        if any([not param for param in [host, port, dbname, username, password]]):
            return {}

        return {
            "host": host,
            "port": port,
            "dbname": dbname,
            "username": username,
            "password": password,
        }

    def _generate_workload_pebble_layer(self, db_config):
        """Generates the Waltz layer for Pebble.

        The generated layer will also contain the database configuration required by FINOS Waltz
        into the environment.
        """
        return {
            "summary": "Waltz layer",
            "description": "Pebble config layer for Waltz.",
            "services": {
                "waltz": {
                    "override": "replace",
                    "summary": "waltz",
                    "command": "/bin/sh -c 'docker-entrypoint.sh update run'",
                    "startup": "enabled",
                    "user": "waltz",
                    "environment": {
                        "DB_HOST": db_config["host"],
                        "DB_PORT": db_config["port"],
                        "DB_NAME": db_config["dbname"],
                        "DB_USER": db_config["username"],
                        "DB_PASSWORD": db_config["password"],
                        "DB_SCHEME": "waltz",
                        "WALTZ_FROM_EMAIL": "help@finos.org",
                        "CHANGELOG_FILE": "/opt/waltz/liquibase/db.changelog-master.xml",
                    },
                }
            },
        }


if __name__ == "__main__":
    main.main(WaltzOperatorCharm)
