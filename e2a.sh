#!/bin/bash

#########################################################################################
 # Program:     AS400 e2a converted wrapper
 # Description: Wrapper for EBCDIC-To-ASCII Converter with packed fields (COMP-3) management 
 #              for Codepage 273 (Germany). 
 # Author:      Adrian Dominiczak, adrian.dominiczak@extern.isban.com
 #				Isban DE GmbH, Digital Unit - Big Data,
 # Date:        2018-01-09
 #
 # #Execution:   ./e2a-wrapper.sh -i INPUT_FILE_PATTERN -o OUTPUT_FILE_PATH -m OUTPUT_METADATA_FILE_PATH -d DATABASE [-z]
 # 				INPUT_FILE_PATTERN: file path + file pattern used to do the FTP before running this script. 
 #				OUTPUT_FILE_PATH: file path for converted file. 
 #				OUTPUT_METADATA_FILE_PATH: file path for result metadata of converted file.
 #				DATABASE: name of database that will be appended to each record in output meta file.
 #              z: enable debug (optional)
 # example call:
 # 
 # /../../e2a-wrapper.sh -i "/../../SALDEN311.20180313*" -o /../../SALDEN311.20180313*.txt -m /../../metadata/SALDEN311.csv -d bu_as400_leasing_hist -z
 #
 # #Clearing after copying from Windows host: 	sed -i -e 's/\r$//' e2a-wrapper.sh
 # 				
 #
 # Change History
 # Version    By         	Date        Change
 # #1.0       Dominiczak    2018-01-09  initial version
 # #1.1       Vaehsen       2018-03-14  changes for control m 
 # #1.2       Vaehsen       2018-03-15  adding UUID and z(debug) parameter
 ########################################################################################/
# Parameters.
INPUT_FILE_PATTERN=""
OUTPUT_FILE_PATH=""
OUTPUT_METADATA_FILE_PATH=""
DATABASE=""

# Auxiliary flags
INPUT_FILE_FLAG=0
OUTPUT_FILE_FLAG=0
OUTPUT_METADATA_FILE_FLAG=0
DATABASE_FLAG=0
DEBUG_FLAG=0

# Read input parameters.
while getopts ':i:o:m:d:z' option; do
	case ${option} in
		i) 
			INPUT_FILE_PATTERN=$OPTARG
			INPUT_FILE_FLAG=1
			echo $INPUT_FILE_PATTERN
			;;
		o)	OUTPUT_FILE_PATH=$OPTARG
			OUTPUT_FILE_FLAG=1
                        echo $OUTPUT_FILE_PATH
			;;
		m)
			OUTPUT_METADATA_FILE_PATH=$OPTARG
			OUTPUT_METADATA_FILE_FLAG=1
                        echo $OUTPUT_METADATA_FILE_PATH
			;;
		d) 
			DATABASE=$OPTARG
			DATABASE_FLAG=1
                        echo $DATABASE
			;;
		z)
			DEBUG_FLAG=1
			;;
		/?) 
			echo "Invalid option: $OPTARG"
			exit -1
			;;
		:)	
			echo "Option: $OPTARG requires a value"
			exit -1
			;;
	esac
done

WORKING_DIR=$(dirname "$0") # path where the script is located
LOG_FILE="$WORKING_DIR/converter-$(date '+%Y-%m-%d').log"
UUID=$(uuidgen)


if [ -n "$(lsof $INPUT_FILE_PATTERN)" ]; then
	echo $@
	echo "File opened, waiting"
	sleep 160
fi

sleep 80

echo $(date '+%Y-%m-%d %T.%3N') "$UUID [DEBUG]: DEBUG enabled"
if [ $DEBUG_FLAG -eq 1 ];then
	rm $LOG_FILE
fi


echo "Log file is: $LOG_FILE"
echo "UUID of current execution is: $UUID"
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: AS400 e2a converted wrapper starts!..." >> $LOG_FILE;
# Check if all parameters flags were provided. No checking of parameters values yet. 
# Check for INPUT FILE PATH. DAILY BATCH FILE.
if [ "$INPUT_FILE_FLAG" -eq 0 ];
	then echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: NO INPUT_FILE_PATTERN SPECIFIED!" >> $LOG_FILE;
	echo $(date '+%Y-%m-%d %T.%3N') " Usage: " >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') "./e2a-wrapper.sh -i INPUT_FILE_PATTERN -o OUTPUT_FILE_PATH -m OUTPUT_METADATA_FILE_PATH -d DATABASE" >> $LOG_FILE;
	exit -1
fi
# Check for OUTPUT FILE PATH. CONVERTED DAILY BATCH FILE.
if [ "$OUTPUT_FILE_FLAG" -eq 0 ];
	then echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: NO OUTPUT_FILE_PATH SPECIFIED!" >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') " Usage: " >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') "./e2a-wrapper.sh -i INPUT_FILE_PATTERN -o OUTPUT_FILE_PATH -m OUTPUT_METADATA_FILE_PATH -d DATABASE" >> $LOG_FILE;
	exit -1
fi
# Check for OUTPUT METADATA FILE PATH. CONVERTED DAILY BATCH FILE.
# Check for OUTPUT FILE PATH. CONVERTED DAILY BATCH FILE.
if [ "$OUTPUT_FILE_FLAG" -eq 0 ];
	then echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: NO OUTPUT_FILE_PATH SPECIFIED!" >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') " Usage: " >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') "./e2a-wrapper.sh -i INPUT_FILE_PATTERN -o OUTPUT_FILE_PATH -m OUTPUT_METADATA_FILE_PATH -d DATABASE" >> $LOG_FILE;
	exit -1
fi
# Check for OUTPUT METADATA FILE PATH. CONVERTED DAILY BATCH FILE.
if [ "$OUTPUT_METADATA_FILE_FLAG" -eq 0 ];
	then echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: NO OUTPUT_METADATA_FILE_PATH SPECIFIED!" >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') " Usage: " >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') "./e2a-wrapper.sh -i INPUT_FILE_PATTERN -o OUTPUT_FILE_PATH -m OUTPUT_METADATA_FILE_PATH -d DATABASE" >> $LOG_FILE;
	exit -1
fi
# Check for DATABASE NAME. 
if [ "$DATABASE_FLAG" -eq 0 ];
	then echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: NO DATABASE specified. DATABASE value setting to $DATABASE!" >> $LOG_FILE;
	echo $(date '+%Y-%m-%d %T.%3N') " Usage: " >> $LOG_FILE; 
	echo $(date '+%Y-%m-%d %T.%3N') "./e2a-wrapper.sh -i INPUT_FILE_PATTERN -o OUTPUT_FILE_PATH -m OUTPUT_METADATA_FILE_PATH -d DATABASE" >> $LOG_FILE;
	exit -1
fi
# Loging.
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: INPUT WAS SET TO: $INPUT_FILE_PATTERN" >> $LOG_FILE
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: OUTPUT WAS SET TO: $OUTPUT_FILE_PATH" >> $LOG_FILE
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: OUTPUT META WAS SET TO: $OUTPUT_METADATA_FILE_PATH" >> $LOG_FILE
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: DATABASE WAS SET TO: $DATABASE" >> $LOG_FILE

file_found=0
# find matching file, since INPUT_FILE_PATTERN is filled with a wildcard
for f in $INPUT_FILE_PATTERN; do
	if [ $file_found -gt 0 ];  
	then 
		echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: More than one file found matching the pattern." >> $LOG_FILE
		echo $INPUT_FILE_PATTERN
		exit -1
	fi
	INPUT_FILE_PATTERN=$f;
	
	file_found=1;
done
if [ "${INPUT_FILE_PATTERN: -1}" = "*" ];
then
	echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: No matching file found for pattern: $INPUT_FILE_PATTERN" >> $LOG_FILE
	exit -1
fi

echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Pattern matched to: $INPUT_FILE_PATTERN" >> $LOG_FILE
# Make unzipping only in case of zipped file. 
# Assumption: zipped or not zipped files will have no .gz extension in the name. 
# If file is not zipped, skip that part and go stright to the matching metadata file.
file_description="$(file $INPUT_FILE_PATTERN)"; 
if [[ $file_description = *'compressed'* ]]; 
then
	echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Found compressed input file. Decompressing starts..."  >> $LOG_FILE
	
	# Check if files not ends with .gz and if adding .gz is necessary. 
	if [[ $INPUT_FILE_PATTERN != *.gz ]]; then
		echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Found no .gz extension during decompressing. Adding .gz extension..."  >> $LOG_FILE
		mv $INPUT_FILE_PATTERN $INPUT_FILE_PATTERN.gz
		echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Extension .gz added. File renamed in place."  >> $LOG_FILE
	fi
	# Actual decompression. 
	gunzip $INPUT_FILE_PATTERN.gz
	echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Decompression. [DONE]" >> $LOG_FILE
elif [[ $INPUT_FILE_PATTERN == *.gz ]];
then
	# Not compressed file, with .gz extension. 
	echo $(date '+%Y-%m-%d %T.%3N') "$UUID [ERROR]: Found uncompressed input file but with gz extension. Check!."  >> $LOG_FILE
	exit -1
else
	echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Given file was proper. No pre-processing done."  >> $LOG_FILE
fi

OUTPUT_FILE_PATH="$(dirname $OUTPUT_FILE_PATH)/$(basename $INPUT_FILE_PATTERN).$(cut -d '.' -f 3 <<< "$OUTPUT_FILE_PATH" )"
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: OUTPUT_FILE_PATH is set to: "$OUTPUT_FILE_PATH >> $LOG_FILE

# Match input file with proper input metadata file. 
# Example: 
# /ingestion/data/in/HIKO/HIKO.20171201235553911093 -> /ingestion/bin/converter/metadata/hiko.md
META="$WORKING_DIR/metadata/$(echo "$(cut -d '.' -f 1 <<< "$(basename $INPUT_FILE_PATTERN)" )"  | tr '[:upper:]' '[:lower:]').md"
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: name of matched metadata file: "$META >> $LOG_FILE


# Call converter to obtain file in ASCII format.
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Before calling converter... Log parameters:" >> $LOG_FILE
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: $WORKING_DIR/e2a $INPUT_FILE_PATTERN $OUTPUT_FILE_PATH $OUTPUT_METADATA_FILE_PATH $META $DATABASE" >> $LOG_FILE
echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: Calling e2a converter... " >> $LOG_FILE

$WORKING_DIR/e2a $INPUT_FILE_PATTERN $OUTPUT_FILE_PATH $OUTPUT_METADATA_FILE_PATH $META $DATABASE $UUID >> $LOG_FILE && echo $(date '+%Y-%m-%d %T.%3N') "$UUID [INFO]: AS400 e2a converted wrapper finished!... [DONE]" >> $LOG_FILE;
