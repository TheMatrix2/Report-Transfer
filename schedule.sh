#!/bin/bash

mkdir -p ~/yandex_metrika_etl

cp yandex-metrika-to-mariadb.py ~/yandex_metrika_etl/

mkdir -p ~/yandex_metrika_etl/logs

cat > ~/yandex_metrika_etl/run_etl.sh << 'EOF'
#!/bin/bash
# Script to run the ETL process and log output

# Set the path to the Python script
SCRIPT_PATH=~/yandex_metrika_etl/yandex-metrika-to-mariadb.py

# Set the log file path
LOG_DIR=~/yandex_metrika_etl/logs
LOG_FILE=$LOG_DIR/etl_$(date +\%Y\%m\%d_\%H\%M\%S).log

# Ensure the log directory exists
mkdir -p $LOG_DIR

# Run the ETL script
echo "Starting ETL process at $(date)" > $LOG_FILE
python3 $SCRIPT_PATH 7 >> $LOG_FILE 2>&1
echo "ETL process completed at $(date)" >> $LOG_FILE

# Clean up old log files (keep only last 10)
ls -t $LOG_DIR/etl_*.log | tail -n +11 | xargs -r rm
EOF

chmod +x ~/yandex_metrika_etl/run_etl.sh

(crontab -l 2>/dev/null; echo "0 1 * * 0 ~/yandex_metrika_etl/run_etl.sh") | crontab -

echo "Cron job set up to run every Sunday at 1 AM"
echo "You can check the status of cron jobs with: crontab -l"