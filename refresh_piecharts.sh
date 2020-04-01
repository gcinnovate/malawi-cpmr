#!/usr/bin/env bash
# Run this script as a cronjob that runs every 30 minutes
# If in the /root/ directory - this is how we set the cronjob
# */30 * * * *  /root/refresh_piecharts.sh
cd /var/www/cpmr-prod/malawi-cpmr
source /var/www/envs/cpmr/bin/activate
flask refresh-pvsu-casetypes
