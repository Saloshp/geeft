#!/bin/bash

SPOOL_DIR=$(cat ./geeft.yml | grep spool_dir | awk '{print $2}')
mkdir -p ${SPOOL_DIR}

#cp ./geeft /usr/local/bin/geeft
cp ./geeft.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable geeft.service
systemctl restart geeft.service

