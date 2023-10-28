#!/bin/bash

cd /home/ubuntu/scripts/

bash ./truncate-table.sh

# import source .env
source .env

y_now=$(date +%Y)
m_ago=$(date --date='3 month ago' +%m)
m_ago_4=$(date --date='4 month ago' +%m)

#date_ago=$y_now-$m_now-01" "00:00:00
date_ago=$y_now-$m_ago-01" "00:00:00

# List of tables to process
TABLES=("table1" "table2" "table3")


# Log file
LOG_FILE=backup.log

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
    echo "$(current_datetime) - Start Processing table: $table" >> $LOG_FILE
    
    # Step 1: Select count(*)
    step1_query="SELECT count(*) FROM $table"
    execute_query "$step1_query"
    echo "$(current_datetime) - count(s) the all total of rows in the $table = $r1" >> $LOG_FILE
    
    # Step 2: Select count(*) with a condition
    step2_query="SELECT count(*) FROM $table WHERE created_at < '$date_ago'"
    execute_query "$step2_query"
    echo "$(current_datetime) - count(s) rows the $table = $r1" >> $LOG_FILE
    
    # Step 3: Insert into a new table
    if [ table3 == $table ]; then
	   step3_query="INSERT INTO ${table}${suffix} SELECT id,created_at FROM $table WHERE created_at < '$date_ago'"
	   execute_query "$step3_query"
	   echo "$(current_datetime) - success added data from $table to ${table}${suffix}" >> $LOG_FILE
    else
     step3_query="INSERT INTO ${table}${suffix} SELECT * FROM $table WHERE created_at < '$date_ago'"
	   execute_query "$step3_query"
     echo "$(current_datetime) - success added data from $table to ${table}${suffix}" >> $LOG_FILE
    fi
    
    # Step 4: Select count from the new table
    step4_query="SELECT count(*) FROM ${table}${suffix}"
    execute_query "$step4_query"
    echo "$(current_datetime) - count(s) rows the ${table}${suffix} = $r1" >> $LOG_FILE
    
    # Step 5: Perform a pg_dump of the new table
    pg_dump "$DB_NAME" -t "${table}${suffix}" -h $DB_HOST -p $DB_PORT -U $DB_USER -f "${table}${suffix}-${m_ago_4}.sql"
    echo "$(current_datetime) - pg_dump pulled down the ${table} to ${table}${suffix}. Done." >> $LOG_FILE
    
    # Step 6: Compress the dump file and remove file
    zip -q9 "${table}${suffix}-${m_ago_4}.zip" "${table}${suffix}-${m_ago_4}.sql" >> $LOG_FILE 2>&1
    echo "$(current_datetime) - zip file $table (sql) to $table (zip) is success" >> $LOG_FILE
    file_size=$(ls -lh ./${table}${suffix}-${m_ago_4}.sql | awk -F " " {'print $5'})
    rm "${table}${suffix}-${m_ago_4}.sql"  # Remove the uncompressed file
    echo "$(current_datetime) - the system successfully deletes the $table (sql) file is size = $file_size" >> $LOG_FILE
    
    # Step 7: Copy to S3
    aws s3 cp "${table}${suffix}-${m_ago_4}.zip" "$S3_BUCKET" --no-progress > /dev/null
    echo "$(current_datetime) - copy $table (zip) to s3 is success" >> $LOG_FILE
    
    # Remove the local compressed file after successful S3 upload
    if [ $? -eq 0 ]; then
        rm -f "${table}${suffix}-${m_ago_4}.zip"
    fi
    
    # Step 8: Check file upload to aws s3 and Send Line notification
    fileBackup=$(aws s3 ls --human-readable --summarize --recursive "${S3_BUCKET_SHORT}" --profile default | grep "${table}${suffix}-${m_ago_4}.zip" |
    awk '{print "file "$5" size : "$3,$4" , Upload to s3"}')
    if [ "$fileBackup" != "" ]; then
        echo "$(current_datetime) - $fileBackup" >> $LOG_FILE
	      line_message="Backup for $table completed and $fileBackup successfully at $(current_datetime)"
    else
        echo "$(current_datetime) - File ${table}${suffix}-${m_ago_4}.zip not found on s3 !!" >> $LOG_FILE
	      line_message="!! Backup for $table failed and not upload on s3 at $(current_datetime)"
    fi
    curl -s -X POST -H "Authorization: Bearer $LINE_TOKEN" -F "message=$line_message" https://notify-api.line.me/api/notify > /dev/null
    echo "$(current_datetime) - End Processing table: $table" >> $LOG_FILE
done
