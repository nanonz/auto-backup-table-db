#!/bin/bash

# import source .env
source .env

# List of tables to process
TABLES=("linepay_bts_trips" "linepay_payments" "linepay_transactions") # for Prod

# Log file
LOG_FILE=truncate.log

# Function Get date & time now iso-8601 (ICT, +07)
current_datetime() {
    date +"%Y-%m-%d %H:%M:%S.%3N"
}

# Function Get date & time now iso-8601 (UTC, +00)
current_datetime_utc() {
    date -u +"%Y-%m-%d %H:%M:%S.%3N"
}

# Function to execute SQL query
execute_query() {
    local query="$1"
    r1=`psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$query"  | awk 'NR==3' | tr -d '[:space:]'`
}

# Loop through tables
for table in "${TABLES[@]}"; do
    echo "$(current_datetime) - Start truncate table: ${table}${suffix}" >> $LOG_FILE

    # Step 1: Select count(*) for table before truncate
    step1_query="SELECT count(*) FROM ${table}${suffix}"
    execute_query "$step1_query"
    echo "$(current_datetime) - row count before table deletion: ${table}${suffix} = $r1" >> $LOG_FILE
    sleep 5
    # Step 2: Truncate table process
    step2_query="TRUNCATE TABLE ${table}${suffix};"
    execute_query "$step2_query"
    echo "$(current_datetime) - truncate table ${table}${suffix} is completed" >> $LOG_FILE
    sleep 5
    # Step 3: Select count(*) for table after truncate
    step3_query="SELECT count(*) FROM ${table}${suffix}"
    execute_query "$step3_query"
    echo "$(current_datetime) - row count before table deletion: ${table}${suffix} = $r1" >> $LOG_FILE
    sleep 5

    echo "$(current_datetime) - End truncate table: ${table}${suffix}" >> $LOG_FILE
done
