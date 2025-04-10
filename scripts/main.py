#!flask/bin/python
# -*- coding: utf-8 -*-
"""
Actinia Core
"""

import os
from src.actinia_satellite_plugin.endpoints import create_endpoints
from actinia_core.core.common.app import flask_app
from actinia_core.core.common.config import global_config, DEFAULT_CONFIG_PATH
from actinia_core.core.common.kvdb_interface import connect
from actinia_core.core.common.process_queue import create_process_queue

__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"

if os.path.exists(DEFAULT_CONFIG_PATH) is True and os.path.isfile(
    DEFAULT_CONFIG_PATH
):
    global_config.read(DEFAULT_CONFIG_PATH)

# Create the endpoints based on the global config
create_endpoints()

# Connect the kvdb interfaces
connect(global_config.KVDB_SERVER_URL, global_config.KVDB_SERVER_PORT)

# Create the process queue
create_process_queue(global_config)


if __name__ == "__main__":
    # Connect to the database
    flask_app.run(host="0.0.0.0", port=8080, debug=True)
