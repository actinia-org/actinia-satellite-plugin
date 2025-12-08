#!/usr/bin/env sh
########################################################################
#
# MODULE:       tests_with_kvdb.sh
#
# AUTHOR(S):    Anika Weinmann
#               mundialis GmbH & Co. KG, Bonn
#               https://www.mundialis.de
#
# PURPOSE:      This script tests the kvdb (valkey) server
#
# SPDX-FileCopyrightText: (c) 2022 by mundialis GmbH & Co. KG
#
# REQUIREMENTS: sudo apt install valkey-server
#
# SPDX-License-Identifier: GPL-3.0-or-later
#
########################################################################

# start kvdb server
valkey-server &
sleep 1
valkey-cli ping

# # start webhook server
# webhook-server --host "0.0.0.0" --port "5005" &
# sleep 10

# run tests
echo $ACTINIA_CUSTOM_TEST_CFG
echo $DEFAULT_CONFIG_PATH

if [ "$1" == "dev" ]
then
  echo "Executing only 'dev' tests ..."
  pytest -m "dev"
elif [ "$1" == "integrationtest" ]
then
  pytest -m "integrationtest"
else
  pytest
fi

TEST_RES=$?

# stop kvdb server
valkey-cli shutdown

# # stop webhook server
# curl http://0.0.0.0:5005/shutdown

return $TEST_RES
