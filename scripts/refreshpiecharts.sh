#!/usr/bin/env bash

# This is run as a cron job every 30 minutes
# */30 * * * *  /root/refresh_piecharts.sh

cd /var/www/cpmr-prod/malawi-cpmr
source /var/www/envs/cpmr/bin/activate
flask refresh-pvsu-casetypes
