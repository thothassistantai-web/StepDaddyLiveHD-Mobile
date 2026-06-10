#!/data/data/com.termux/files/usr/bin/bash
LOGFILE="/sdcard/Download/stepdaddy-runtime.log"
exec > >(tee -a "$LOGFILE") 2>&1
cd "/data/data/com.termux/files/home/stepdaddy-livehd-private-master"
source venv/bin/activate
reflex run --env prod
