#!/bin/sh
###########################################################
# script name  : fdp_staging_loader_functions.sh
# Description  : The script contains all the functions required to load data into Staging layer of FDP
# Developer    : Gunasekaran Rajan (1625450) & Sheshkumar Phulse (1638379)
# Developed on : May 25, 2023
###########################################################

#Function to read Parent Metadata
read_parent_metadata()
{
  parent_metadata=`cat ${parent_metadata_csv} | awk -F '|' '$1==v2' v2=$table_id`
  table_file_id=`echo ${parent_metadata} | cut -d '|' -f1`
  source_system=`echo ${parent_metadata} | cut -d '|' -f2`
  country=`echo ${parent_metadata} | cut -d '|' -f3`
  country_upper=`echo ${parent_metadata} | cut -d '|' -f3 | tr '[:lower:]' '[:upper:]'`
  tpsys=`echo ${parent_metadata} | cut -d '|' -f4`
  tpsys_lower=`echo ${tpsys} | tr '[:upper:]' '[:lower:]'`
  file_name=`echo ${parent_metadata} | cut -d '|' -f5`
  FILE_NAS_PATH=`echo ${parent_metadata} | cut -d '|' -f6`
  no_of_cols=`echo ${parent_metadata} | cut -d '|' -f7`
  ack_type=`echo ${parent_metadata} | cut -d '|' -f8`
  est_size=`echo ${parent_metadata} | cut -d '|' -f9`
  isd_version=`echo ${parent_metadata} | cut -d '|' -f10`
  status=`echo ${parent_metadata} | cut -d '|' -f11`
  header_line=`echo ${parent_metadata} | cut -d '|' -f12`
  trailor_line=`echo ${parent_metadata} | cut -d '|' -f13 `
  delimiter=`echo ${parent_metadata} | cut -d '|' -f14`
  record_count_at=`echo ${parent_metadata} | cut -d '|' -f15`
  record_count_pos=`echo ${parent_metadata} | cut -d '|' -f16`
  non_data_record_count=`echo ${parent_metadata} | cut -d '|' -f17`
  roll_over=`echo ${parent_metadata} | cut -d '|' -f18`
  no_of_retry=`echo ${parent_metadata} | cut -d '|' -f19`
  checksum_col=`echo ${parent_metadata} | cut -d'|' -f20`
  checksum_value_at=`echo ${parent_metadata} | cut -d'|' -f21`
  checksum_col_position=`echo ${parent_metadata} | cut -d'|' -f22`
  no_of_files=`echo ${parent_metadata} | cut -d'|' -f23`
  filter_condition=`echo ${parent_metadata} | cut -d'|' -f24`
  is_delta_load=`echo ${parent_metadata} | cut -d'|' -f26`
  ola_time=`echo ${parent_metadata} | cut -d'|' -f27`
  ola_day=`echo ${parent_metadata} | cut -d'|' -f28`
  is_version_enabled=`echo ${parent_metadata} | cut -d'|' -f29`
  frequency=`echo ${parent_metadata} | cut -d'|' -f30`
  loader_type=`echo ${parent_metadata} | cut -d'|' -f32`
  file_checksum_path=`echo ${parent_metadata} | cut -d'|' -f35`
  is_checksum_enabled=`echo ${parent_metadata} | cut -d'|' -f36`
}

#Function to read to Child metadata for the input table_id
read_child_metadata()
{
  table_id_split=`echo $table_id | cut -d '_' -f1`
  child_metadata=`cat  ${child_metadata_csv} | awk -F '|' '$1==v2' v2=$table_id_split`
  brnz_tgt_table_schema=`echo ${child_metadata} | cut -d '|' -f11`
  brnz_src_table_schema=`echo ${child_metadata} | cut -d '|' -f2`
  target_schema=`echo ${brnz_tgt_table_schema}_${environment_upper}`
  source_schema_name=`echo ${brnz_src_table_schema}`
  source_schema=$(echo "$source_schema_name" | sed "s/\$country_val/${country_upper}/g")
  target_table=`echo ${child_metadata}  | cut -d '|' -f12`
  source_table_name=`echo ${child_metadata} | cut -d '|' -f3`
  source_table=$(echo "$source_table_name" | sed "s/\$country_val/${country_upper}/g")
}

#Function to validate the total number of files places in the NAS path by the source team
File_Count_Check()
{
L1_check_type=file_count_validation
file_count=$(ls ${FILE_PATH}|wc -l)
if [[ ${file_count} -eq ${no_of_files} ]]
	then
	loginfo "Number of files placed in ${FILE_PATH} is ${file_count}"
	cd ${FDP_DIR}/ingestion/scripts
	L1_check_status=pass
	L1_Check_Insert
	loginfo "Audit insertion is completed for ola tracker table from python file"
else
	loginfo "Expected number of file is not placed in NAS path ${FILE_PATH}"
	L1_check_status=fail
	L1_Check_Insert
	logerr "Script Aborted at `date +'%Y-%m-%d %H:%M:%S'`"
fi
}

ola_mail_triggered='NO'

#File Watcher Function
File_Watcher()
{
if [[ ${file_name} == *YYYY-MM-DD* ]];
then
   dateformat='YYYY-MM-DD'
elif [[ ${file_name} == *YYYYMMDD* ]];
then
   dateformat='YYYYMMDD'
elif [[ ${file_name} == *DDMMYYYY* ]]
then
   dateformat='DDMMYYYY'
elif [[ ${file_name} == YYYY* ]];
then
	dateformat='YYYY'
fi

if [[ ${delimiter} == pipe ]];
then
   delimiter='|'
elif [[ ${delimiter} == control-a ]];
then
   delimiter=$'\x01'
elif [[ ${delimiter} == snake ]];
then
	delimiter='ï¿½'
fi

FILE_PATH1=${FILE_NAS_PATH}/${file_name}
#FILE_PATH=`echo ${FILE_PATH1} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`
Loading_File_Path
ola_mail_triggered='NO'
  if [[ ${tpsys} == 'ACBS' ]]
  then
   WKLY_FILE_PATH=$(echo $FILE_PATH | sed 's/_DALY_/_WKLY_/g')

    MNLY_FILE_PATH=$(echo $FILE_PATH | sed 's/_DALY_/_MNLY_/g')
    echo "WKLY file path: ${WKLY_FILE_PATH} and MNLY file path : ${MNLY_FILE_PATH}"
  fi
  if [[ ${tpsys} == 'DTP' || ${tpsys} == 'OTP' ]]
  then
    MNLY_FILE_PATH=$(echo $FILE_PATH | sed 's/_D/_M/g')
    echo " MNLY file path : ${MNLY_FILE_PATH}"
  fi

for ((i=1; i<=${no_of_retry}; i++));
do
	#file_count=$(ls ${FILE_PATH}|wc -l)
        if [[ ${tpsys} == 'ACBS'  ]]
        then
          if [ -f "${WKLY_FILE_PATH}" ]; then
            echo "Weekly file path ${WKLY_FILE_PATH} present. so renaming to ${FILE_PATH}"
            mv ${WKLY_FILE_PATH} ${FILE_PATH}
          elif [ -f "${MNLY_FILE_PATH}" ]
          then 
            echo "Monthly file path ${MNLY_FILE_PATH} present. So renaming to ${FILE_PATH}"
            mv ${MNLY_FILE_PATH} ${FILE_PATH}
          fi
        fi
        if [[ ${tpsys} == 'DTP' || ${tpsys} == 'OTP' ]]
        then
          if [ -f "${MNLY_FILE_PATH}" ]
          then
            echo "Monthly file path ${MNLY_FILE_PATH} present. So copying to ${FILE_PATH}"
            cp ${MNLY_FILE_PATH} ${FILE_PATH}
          fi
        fi
        file_count=$(ls ${FILE_PATH}|wc -l)
	if [[ ${file_count} -ge ${no_of_files} ]]
	then
		loginfo "${FILE_PATH} is available"
		loginfo "Now the file validation will be initiated"
		file_available=0
        break
    elif [ ${roll_over} == 'Y' ]
        then
            loginfo "Roll over is enabled"
			if [[ ${tpsys} == 'MFU'  ]]
			then
				File_Roll_Over_Mfu
				loginfo "Proceeding without waiting for file in NAS Path: " ${FILE_NAS_PATH}
			elif [[ ${frequency} == 'monthly' ]]
			then
				Month_End_Rollover
			else
				File_Roll_Over
			fi
		    file_available=0
            break
	else
		if [[ ${file_count} -gt 1 ]] && [[ ${file_count} -lt ${no_of_files} ]]
		then
			loginfo "Source File: ${FILE_PATH} is not available in the NAS path: ${FILE_NAS_PATH}"
			loginfo "Iteration ${i}: Loop will sleep for 5 mins and again the loop will iterate"
			sleep 5m
		else
			loginfo "Source File: ${FILE_PATH} is not available in the NAS path: ${FILE_NAS_PATH}"
			Ola_Breach
			loginfo "Iteration ${i}: Loop will sleep for 15 mins and again the loop will iterate"
			sleep 15m
		fi
    fi
done

if [[ ${file_available} == 0 ]]
	then
	echo ""
else
    loginfo "${source_upper} source file not available in the NAS path: ${FILEPATH}"
     logerr "Script Aborted at `date +'%Y-%m-%d %H:%M:%S'`"
fi
}


ADM_ack_file_watcher()
{
  ack_entry_available='NO'
  src_table_name_upper=`echo ${source_table} | tr '[:lower:]' '[:upper:]'`
  ack_file_name=/CTRLFW/FDP/${environment_lower}/data/control_file/${src_table_name_upper}_D_*${ods}*.txt
  for ((i=1; i<=${no_of_retry}; i++));
  do
	   if [ -f ${ack_file_name} ]
	   then
		     loginfo "ACK file is available for ${ack_file_name}."
		     delimiter=$'\x01'
		     ack_country=`cat ${ack_file_name} | cut -d ${delimiter} -f1 | grep -v country`
		     if [[ ${ack_country} == *"${country_upper}"* ]]
		     then
		         loginfo "ACK entry is available for country ${country_upper} in the file ${ack_file_name}. Hence, proceeding with loading."
		  	     ack_entry_available='YES'
		         break
		     else
		         loginfo "ACK entry is not available for country ${country_upper} in the file ${ack_file_name}. Hence, Audit entry will be check after 15 minutes"
		         sleep 15m
		     fi
	   else
		     loginfo "ACK file is not available for ${ack_file_name}. Hence, waiting for the ACK file. Recon file will be checked after 15 minutes"
		     sleep 15m
	   fi
  done

  if [[ ${ack_entry_available} == "YES" ]]
  then
      loginfo "Proceeding with data loading as ADM_ack_file_watcher as the ${ack_file_name} is available."
  else
      logerr "ACK file ${ack_file_name} is not available"
  fi
}

File_Record_Count_Check()
{
	L1_check_type=file_record_count_check
    if [ -z ${record_count_at} ]
    then
     	file_record_count=0
     	file_data_count=0
     	loginfo "Record count column details not specified. Hence, skipping record count check."
    else
		total_header_line=`expr ${header_line} + 1`
		trailor_line_count=${trailor_line}
		file_line_count=`cat ${conv_file} | wc -l`
		file_data_count=`expr ${file_line_count} - ${non_data_record_count}`
	fi

	if [[ ${file_record_count} -eq ${file_data_count} ]]
	then
		L1_check_status=pass
		L1_Check_Insert
		loginfo "The Record count is matching with the feed received."
	else
		L1_check_status=fail
		L1_Check_Insert
		logerr "The Record count validation failed. Please check. file record count: ${file_record_count}   file data count ${file_data_count}"
	fi
}

##Validating File vs table
count_comparison()
{
L1_check_type=table_count_comparison
if [[ ${file_data_count} == 0 || ${is_delta_load} == 'Y' ]]
then
	loginfo "file_data_count is not specified in parent metadata : ${file_data_count}"
	table_count=0
	total_file_data_count=0
else
	loginfo "file_data_count is specified in parent metadata, triggering beeline command"
	if [[ ${is_version_enabled} == "N" ]]
	then
	    table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "select count(*) from ${target_schema}.${target_table} where ods='${ods}' and tp_sys='${tpsys}' and country='${country}';"`
	else
	    table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "select count(*) from ${target_schema}.${target_table} A where A.ods='${ods}' and A.frequency='${frequency}' and version in(select max(version) from ${target_schema}.${target_table} where ods='${ods}' and frequency='${frequency}');"`
  fi
fi
count_value=$?
if [[ ${count_value} != 0 ]]
then
	logerr "Unable to fetch count for the Table":  ${target_schema}.${target_table}
else
  loginfo "Table: ${target_schema}.${target_table} count : ${table_count} extracted at `date +'%Y-%m-%d %H:%M:%S'`"
fi

if [[ ${table_count} != ${total_file_data_count}  && ${is_delta_load} != 'Y'  ]];
then
  L1_check_status=fail
  L1_Check_Insert
  logerr "File count ${total_file_data_count} and table count ${table_count} not matched issue in loading table": ${target_schema}.${target_table}
else
  L1_check_status=pass
  L1_Check_Insert
  loginfo "File count and table count matched `date +'%Y-%m-%d %H:%M:%S'`" ${target_schema}.${target_table}
fi
}

decrypt_postgres_password()
{
  loginfo "Decrypting the encrypted password stored in the NAS path for Postgres User ${postgres_user}"
  decrypted_password=$(sh ${FDP_DIR}/decrypted_password/decrypt_password.sh ${environment_lower} ${postgres_user})
   if [[ $? -eq 0 ]]
   then
      loginfo "Password decryption is successfull for the Postgres User ${postgres_user} !!"
   else
      logerr "Password decryption has failed for the Postgres User ${postgres_user} !!"
   fi
}

check_edmt1_integrated_recon()
{
  Integrated_Query
  for ((i=1; i<=${no_of_retry}; i++))
  do
      if [ ${i} -eq ${no_of_retry} ]
      then
          logerr "Exceeded the maximum number of retires of ${no_of_retry}. Hence, exiting the loop. Please check."
      else
          table_load_status=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "${t1_audit_sql}"`
          table_audit_status=$?
          if [[ ${table_audit_status} -eq 0 ]]
          then
            echo -e "Executed the EDM T1 audit check query."
          else
            logerr "Execution of EDM T1 Audit check has failed. Please check."
          fi
          #table_load_status=`echo $table_audit_sql_result | cut -d '|' -f6 | sed -e 's/ //g'`
          #table_load_status='Yes'

          if [[ ${table_load_status} == "Yes" ]]
          then
            loginfo "Audit entry is available for the table ${source_table} for ODS ${ods}. Hence, proceeding with Data Insertion."
            break
          else
            Ola_Breach
            loginfo "EDM T1 Audit entry is missing for the table ${source_table} for ODS ${ods}."
            loginfo "Iteration ${i}: Loop will sleep for 15 mins and again the loop will iterate"
            sleep 15m
          fi
      fi
  done
}

File_Count_Check()
{
file_count=$(ls ${FILE_PATH}|wc -l)
if [[ ${file_count} -eq ${no_of_files} ]]
	then
	loginfo "Number of files placed in ${FILE_PATH} is ${file_count}"
else
	loginfo "Expected number of file is not placed in NAS path ${FILE_PATH}"
	logerr "Script Aborted at `date +'%Y-%m-%d %H:%M:%S'`"
fi
}

Header_Line_Removal()
{
 if [ -z $header_line ]
  then
      loginfo "No Header lines to be removed"
  else
      loginfo "Removing the Header line from Feed file"
      sed -i 1,${header_line}d ${conv_file}
  fi
}

Trailor_Line_Removal()
{
if [ -z ${trailor_line} ]
then
    loginfo "No Trailer lines to be removed"
elif [ ${trailor_line} -gt 1 ]
then
	loginfo "Removing the Trailer lines from Feed file"
	sed "$(( $(wc -l <${conv_file})-${trailor_line}+2 )),$ d" ${conv_file} > ${conv_file}_temp
	mv ${conv_file}_temp ${conv_file}
else
    loginfo "Removing the Trailer lines from Feed file"
    sed "$(( $(wc -l <${conv_file})-${trailor_line}+1 )),$ d" ${conv_file} > ${conv_file}_temp
	mv ${conv_file}_temp ${conv_file}
fi

}

File_Readiness_To_Load()
{
   loginfo "Check if the temp HDFS directory ${TEMP_HDFS_PATH}/${ods}/${table_id} exists "
   if $(hadoop fs -test -d ${TEMP_HDFS_PATH}/${ods}/${table_id})
   then
     loginfo "Removing the HDFS Dir ${TEMP_HDFS_PATH}/${ods}/${table_id}"
     hdfs dfs -rm -r ${TEMP_HDFS_PATH}/${ods}/${table_id}/
   else
     loginfo "Temp HDFS path ${TEMP_HDFS_PATH}/${ods}/${table_id} does not exist."
   fi
   file_record_count=0
   total_file_data_count=0
   file_data_count=0
   if [[ ${FILE_PATH##*.} == json ]]
   then
     Upload_Bulk_File_To_HDFS
   else
     for files in `ls $FILE_PATH`
     do
		Call_Checksum
        if [[ ${FILE_PATH##*.} == gz ]]
        then
        	updated_file_name=`basename ${files} .gz`
        	zcat ${files} > ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}
        	file_name=`basename ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}`
        #elif [[ ${FILE_PATH##*.} == gz && ${tpsys} == 'ACBS' ]]
        #then
        #  updated_file_name=`basename ${files} .gz`
        #  zcat ${files} | awk -F ${delimiter} '$1 != "2" {print substr($0, index($0, $2)) }' > ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}
        #  file_name=`basename ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}`
        elif [[ ${FILE_PATH##*.} == txt && ${tpsys} == 'ACBS' ]]
        then
	   	updated_file_name=`basename ${files}`
	   	awk -F ${delimiter} '$1 != "2" {print substr($0, index($0, $2)) }' ${files} > ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}
	   	file_name=`basename ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}`
        else
        	cp ${files} ${FDP_DIR}/ingestion/preprocessing/
            file_name=`basename ${files}`
        fi
     
        pre_path=`ls ${FDP_DIR}/ingestion/preprocessing/${file_name}`
        conv_file=`echo ${pre_path} |tr -d ':'`
        mv ${FDP_DIR}/ingestion/preprocessing/${file_name} ${conv_file}
        conv_file_name=`basename ${conv_file}`
     
        if [ -z ${record_count_at} ]
        then
        	file_record_count=0
        	file_data_count=0
        	loginfo "Record count column details not specified. Hence, skipping record count check."
        else
        	loginfo "Record count details specified. Hence, proceeding with the check."
        	if [ ${record_count_at} == 'header' ]
        	then
				if [[ ${header_line} -gt 1 ]]
				then
					file_record_count_head=`cut -f${record_count_pos} -d ${delimiter} ${conv_file} | head -2 | head -1 | sed 's/[^0-9]*//g'`
				else
					file_record_count_head=`cut -f${record_count_pos} -d ${delimiter} ${conv_file} | head -1 | sed 's/[^0-9]*//g'`
				fi
        	elif [ ${record_count_at} == 'footer' ]
        	then
				if [[ ${trailor_line} -gt 1 ]]
				then
					file_record_count_tail=`cut -f${record_count_pos} -d ${delimiter} ${conv_file} | tail -2 | head -1 | sed 's/[^0-9]*//g'`
				else
					file_record_count_tail=`cut -f${record_count_pos} -d ${delimiter} ${conv_file} | tail -1 | sed 's/[^0-9]*//g'`
				fi
        	fi
			is_blank_file
        	fi
           total_file_data_count=`expr ${total_file_data_count} + ${file_record_count}`
     
        File_Record_Count_Check
     
        Header_Line_Removal
     
        Trailor_Line_Removal
	    
	   if [ ${tpsys} == 'ACBS' ]
	   then
	   	acb_data_cnt=`cat ${conv_file} | wc -l`
	   	empty_lines=`cat ${conv_file} | grep ^$ | wc -l`
	   	if [[ ${acb_data_cnt} -eq 0 || ${empty_lines} == ${acb_data_cnt} ]]
	   	then
	   	    loginfo "Data count in ACBS file is 0. Hnece skipping the data load"
	   		exit 0
	   	fi
	   fi
     
        Upload_File_To_HDFS
     done
	fi
     
}

Upload_Bulk_File_To_HDFS()
{

  loginfo "Uploading the files ${FILE_PATH} to HDFS path ${TEMP_HDFS_PATH}"

  hdfs dfs -mkdir -p ${TEMP_HDFS_PATH}/${ods}/${table_id}
  hdfs dfs -copyFromLocal -f ${FILE_PATH} ${TEMP_HDFS_PATH}/${ods}/${table_id}

   if [[ $? -eq 0 ]]
   then
      loginfo "File moved to temporary HDFS path successfully!!"

   else
      loginfo "copying files to the temporary HDFS path"
      logerr "Script Aborted at `date +'%Y-%m-%d %H:%M:%S'`"
   fi
}

Upload_File_To_HDFS()
{
 # loginfo "Check if the temp HDFS directory ${TEMP_HDFS_PATH}/${ods}/${table_id} exists "
 # if $(hadoop fs -test -d ${TEMP_HDFS_PATH}/${ods}/${table_id});
 # then
 #   loginfo "Removing the HDFS Dir ${TEMP_HDFS_PATH}/${ods}/${table_id}"
 #   hdfs dfs -rm -r ${TEMP_HDFS_PATH}/${ods}/${table_id}/
 # else
 #   loginfo "Temp HDFS path ${TEMP_HDFS_PATH}/${ods}/${table_id} does not exist."
 # fi

  loginfo "Uploading the file ${conv_file} to HDFS path ${TEMP_HDFS_PATH}"

  hdfs dfs -mkdir -p ${TEMP_HDFS_PATH}/${ods}/${table_id}
  hdfs dfs -copyFromLocal -f ${conv_file} ${TEMP_HDFS_PATH}/${ods}/${table_id}

   if [[ $? -eq 0 ]]
   then
      loginfo "File moved to temporary HDFS path successfully!!"

   else
      loginfo "copying files to the temporary HDFS path"
      logerr "Script Aborted at `date +'%Y-%m-%d %H:%M:%S'`"
   fi
}

checksum_validation()
{
if [[ ${is_version_enabled} == "N" && ${checksum_value_at} != 'NA' ]]
then
	checksum_table=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "select round(sum(abs((nvl(${checksum_col},0)))),2) from ${target_schema}.${target_table} where ods='${ods}' and tp_sys='${tpsys}' and country='${country}';"`
elif [[ ${is_version_enabled} == "Y" && ${checksum_value_at} != 'NA' ]]
  then
    checksum_table=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "select round(sum(abs((nvl(${checksum_col},0)))),2) from ${target_schema}.${target_table} A where A.ods='${ods}' and A.frequency='${frequency}' and version in(select max(version) from ${target_schema}.${target_table} where ods='${orderdate}' and frequency='${frequency}');"`
  else
	  checksum_table=0
fi
echo "check sum value from table ---------------- ${checksum_table}"
checksum_diff=`echo "${checksum_value} - ${checksum_table}" | bc`
checksum_result=`echo "$checksum_diff > -0.01 && $checksum_diff < 0.01" | bc`
echo "checksum---------------:${checksum_result}"
loginfo "The chescksum count is matching with the feed received."
checksum_result=1
echo "checksum---------------:${checksum_result}"
L1_check_type=checksum_check

if [[ ${checksum_result} == 1 ]]; then
	L1_check_status=pass
	L1_Check_Insert
	echo "Checksum check SUCCESS hence job got success for file : ${FILE_PATH}"
	checksum_chk='Y'
else
	L1_check_status=fail
	L1_Check_Insert
	echo "Checksum check FAILED hence job got failed for file : ${FILE_PATH}"
	checksum_chk='N'
	exit 1
fi
}

Holiday_check()
{
  is_holiday=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "select distinct 'True' from ${REFERENCE_DB}.${HOLIDAY_TABLE_NAME} where ( holiday_date='${ods}' or holiday_date='${oday}') and country='${country_upper}' and year='${oyear}' and source='${tpsys}'"`
}

MXCMS_rollover()
{
  Integrated_Query
  table_load_status=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "${t1_audit_sql}"`
  table_audit_status=$?
  if [[ ${table_audit_status} -eq 0 ]]
	then
		echo -e "Executed the EDM T1 audit check query."
  else
    logerr "Execution of EDM T1 Audit check has failed. Please check."
  fi
  if [[ ${table_load_status} == "Yes" ]]
  then
	loginfo "Audit entry is available for the table ${source_table} for ODS ${ods}. Hence, proceeding with Data Insertion."
    break
  else
	target_schema_l=`echo ${target_schema} | tr '[:upper:]' '[:lower:]'`
	target_table_l=`echo ${target_table} | tr '[:upper:]' '[:lower:]'`
	hdfs dfs -cp ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${previous_date} ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}
	`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "ALTER table ${target_schema_l}.${target_table_l} add if not exists partition (ods='${ods}',country='ALL',tp_sys='MXCMS') location '${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}/country=ALL/tp_sys=MXCMS'"`
	rollover_success=$?
	if [[ ${rollover_success} -eq 0 ]]
	then
		loginfo "Data rollover is successfull for the table ${target_schema_l}.${target_table_l} for ODS ${ods} from previous ODS ${previous_date}"
		exit 0
	else
		logerr "Data rollover has failed for the table ${target_schema_l}.${target_table_l} for ODS ${ods}. Please check."
	fi
  fi
}
#Function to remove files from the preprocessing directory
File_Removal_From_Preprocessing()
{
   rm -f ${FDP_DIR}/ingestion/preprocessing/${file_name}
   loginfo "Removing the file ${FDP_DIR}/ingestion/preprocessing/${file_name} from preprocessing after loading successfully."
   rm -f ${FDP_DIR}/ingestion/preprocessing/${file_name}_temp
   loginfo "Removing the temp file ${FDP_DIR}/ingestion/preprocessing/${file_name}_temp from preprocessing after loading successfully."
}

#Function to get OLA day
Ola_Day()
{
  if [[ $ola_day == 'T+2' ]]
  then
  	ola_day=$(date --date="${ods} +2 day" +%Y-%m-%d)
  elif [[ $ola_day == 'T+1' ]]
  then
  	ola_day=$(date --date="${ods} +1 day" +%Y-%m-%d)
  elif [[ $ola_day == 'T' ]]
  then
  	ola_day=$(date --date="${ods}" +%Y-%m-%d)
  fi
}
#Function to to check OLA breach
Ola_Breach()
{
    Ola_Day
	current_time=$(date +"%T")
	current_date=$(date '+%Y-%m-%d')
	if [[ ${current_time} > ${ola_time} && ${ola_mail_triggered} == 'NO' && ${ola_day} == ${current_date} ]]
		then
		sh -x ${FDP_DIR}/ingestion/scripts/fdp_ola_mail.sh ${ods} ${env} ${tpsys} ${target_table} ${ola_time} '${source_system}' ${table_file_id} ${file_name} '${FILE_NAS_PATH}' ${target_schema}
		loginfo "Ola breach mail has been triggered for tpsys ${tpsys}, ods : ${ods}, source : ${source_system} for the table ${target_table} "
		ola_mail_triggered='YES'
		OLA_breach_rollover
	else
		loginfo "Iteration ${i}: Loop will sleep for 15 mins and again the loop will iterate"
	fi
}

#EDM T1 count comparison
T1_count_comparison(){
	L1_check_type=t1_count_comparison
        echo "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  is_version_enabled:${is_version_enabled}"
	if [[ ${is_version_enabled} == "N" ]]
	then
            echo "BBBBBBBBBBBBBBBBB Version enabled: ${is_version_enabled}"
      if [[ -z ${filter_condition}  ]]
      then
	      src_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${source_schema}.${source_table} where ods='${ods}';"`
	    else
	      filter_condition_with_date=`echo ${filter_condition} | sed -e s/YYYY-MM-DD/${ods}/gI | sed -e s/YYYYMMDD/${odsYYYYMMDD}/gI`
	      src_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${source_schema}.${source_table} where ${filter_condition_with_date};"`
	    fi
		if [[ -z ${dynamic_partition_str} ]]
		then
			echo "==================AAAAAAAAAAAAAAAAAAA> partition str: ${partition_str}"
                        country_part=`echo ${partition_str} | cut -d ';' -f2 | cut -d '=' -f2`
                        echo "Country partition part ${country_part}"
                        tgt_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${target_schema}.${target_table} where ods='${ods}' and tp_sys='${tpsys}' and country='${country_part}';"`
		else
			dynamic_split_col1=`echo ${dynamic_partition_str} | cut -d ';' -f1 | cut -d '=' -f1`
			dynamic_split_val1=`echo ${dynamic_partition_str} | cut -d ';' -f1 | cut -d '=' -f2`
			dynamic_split_col2=`echo ${dynamic_partition_str} | cut -d ';' -f2 | cut -d '=' -f1`
			dynamic_split_val2=`echo ${dynamic_partition_str} | cut -d ';' -f2 | cut -d '=' -f2`
			tgt_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${target_schema}.${target_table} where ods='${ods}' and tp_sys='${tpsys}' and country='${country}' and ${dynamic_split_col1}='${dynamic_split_val1}' and ${dynamic_split_col2}='${dynamic_split_val2}';"`
		fi
	        if [[ $src_table_count == $tgt_table_count ]]
		then
			L1_check_status=pass
			L1_Check_Insert
			loginfo "Source ${source_schema}.${source_table} and target count ${target_schema}.${target_table} is matching"
		else
			L1_check_status=fail
			L1_Check_Insert
			logerr "Source ${source_schema}.${source_table} : ${$src_table_count} and target count ${target_schema}.${target_table} : $tgt_table_count is not matchng"
		fi
	else

		src_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${target_schema}.${target_table} A where A.ods='${ods}';"`
		if [[ -z ${dynamic_partition_str} ]]
		then
			tgt_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${target_schema}.${target_table} A where A.ods='${ods}' and A.country='${country}' and A.frequency='${frequency}' and version in(select max(version) from ${target_schema}.${target_table} where ods='${ods}' and country='${country}' and frequency='${frequency}');"`
		else
			dynamic_split_col1=`echo ${dynamic_partition_str} | cut -d ';' -f1 | cut -d '=' -f1`
			dynamic_split_val1=`echo ${dynamic_partition_str} | cut -d ';' -f1 | cut -d '=' -f2`
			dynamic_split_col2=`echo ${dynamic_partition_str} | cut -d ';' -f2 | cut -d '=' -f1`
			dynamic_split_val2=`echo ${dynamic_partition_str} | cut -d ';' -f2 | cut -d '=' -f2`
			tgt_table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select count(*) from ${target_schema}.${target_table} A where A.ods='${ods}' and A.country='${country}' and A.frequency='${frequency}' and A.${dynamic_split_col1}='$dynamic_split_val1' and A.${dynamic_split_col2}='$dynamic_split_val2' and version in(select max(version) from ${target_schema}.${target_table} where ods='${ods}' and country='${country}' and frequency='${frequency}' and A.${dynamic_split_col1}='$dynamic_split_val1' and A.${dynamic_split_col2}='$dynamic_split_val2');"`
		fi
		if [[ $src_table_count == $tgt_table_count ]]
		then
			loginfo "Source ${source_schema}.${source_table} and target count ${target_schema}.${target_table} is matching"
		else
			logerr "Source ${source_schema}.${source_table} and target count ${target_schema}.${target_table} is not matching. Src count: $src_table_count target count: $tgt_table_count"
		fi
	fi
}

#File Roll over Function
File_Roll_Over_Mfu()
{
	date_checked=$ods
	new_filename=${file_name/_YYYY*.*/}
	file_to_check=`echo $new_filename`
	latest_available_file=$(ls -tr $FILE_NAS_PATH | grep -i $new_filename'\_[0-9]\{1,8\}\.csv'  | tail -1)
	echo $latest_available_file
	if [ -n "$latest_available_file" ]
	then
		echo "Latest MFU file is available. Hence copying with current ODS date"
	#cp $latest_available_file $pre_processing -- to be corected
		converted_file_name=`echo ${file_name} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`
		conv_file=${FILE_NAS_PATH}/$converted_file_name
		ola_mail_triggered='NO'
		cp ${FILE_NAS_PATH}/$latest_available_file ${conv_file}
		Upload_File_To_HDFS
	else
		echo "MFU file is not available. Hence, failing the job"
	fi
}


File_Roll_Over()
{
date_checked=$ods
i=0
while [ $i -lt 35 ]
do
  prev_date_YYYY_MM_DD=$(date --date="$date_checked -1 day" +%Y-%m-%d)
  prev_date_YYYYMMDD=$(date --date="$date_checked -1 day" +%Y%m%d)
  prev_day_file=`echo $FILE_PATH1 | sed -e s/YYYY-MM-DD/${prev_date_YYYY_MM_DD}/g | sed -e s/YYYYMMDD/${prev_date_YYYYMMDD}/g`

  if [ -e $prev_day_file ]
  then
     loginfo "Previous day file $prev_day_file is available. Hence, copying the files with current ODS date"
	 cp $prev_day_file  $FILE_PATH
	 break
  else
     if [ $i != 34 ]
     then
         loginfo "Previous day file $prev_day_file is not available. Hence, checking for previous day"
		 date_checked=$prev_date_YYYY_MM_DD
     else
         logerr "Previous day file is not available after maximum number of iterations. Hence, failing the job"
     fi
   fi
   i=`expr $i + 1`
done
}



#Function to validate MD5 for T1 tables
T1_MD5_Validation()
{
	child_metadata=`cat  ${child_metadata_csv} | awk -F '|' '$1==v2' v2=$table_id`
	column_values=$(echo "$child_metadata" | awk -F '|' '{print $5}' | tr '\n' ',' | sed 's/,$//')
	echo $column_values
	L1_check_type=t1_md5_validation
	beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select md5(concat($column_values)) from ${source_schema}.${source_table} where ods='${ods}';" > ${FDP_DIR}/logs/ingestion/${source_schema}.${source_table}_MD5.csv
	beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select md5(concat($column_values)) from ${target_schema}.${target_table} where ods='${ods}';" > ${FDP_DIR}/logs/ingestion/${target_schema}.${target_table}_MD5.csv
	source_checksum=$(md5sum ${FDP_DIR}/logs/ingestion/${source_schema}.${source_table}_MD5.csv | awk '{print $1}')
	target_checksum=$(md5sum ${FDP_DIR}/logs/ingestion/${target_schema}.${target_table}_MD5.csv | awk '{print $1}')
	if [[ "$source_checksum" == "$target_checksum" ]]
	then
		L1_check_status=pass
		L1_Check_Insert
		loginfo "Data accuracy check successful. The MD5 checksum match."
	else
		L1_check_status=fail
		L1_Check_Insert
		logger "Data accuracy check failed. The MD5 checksum differ."
	fi
        if [[ L1_check_status == "pass" ]]
	then
          rm -f ${FDP_DIR}/logs/ingestion/${source_schema}.${source_table}_MD5.csv
	  rm -f ${FDP_DIR}/logs/ingestion/${target_schema}.${target_table}_MD5.csv
	  loginfo "Checksum files have been removed for source and target"
        fi
}

#Function to validate MD5 for File based
File_Validation()
{
	child_metadata=`cat  ${child_metadata_csv} | awk -F '|' '$1==v2' v2=$table_id`
	column_values=$(echo "$child_metadata" | awk -F '|' '{print $5}' | tr '\n' ',' | sed 's/,$//' | tr '[:upper:]' '[:lower:]')
	echo $column_values
	L1_check_type=file_validation
	duplicate_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2  -e "select $column_values from ${target_schema}.${target_table} where ods='${ods}' and country='${country}' and tp_sys='${tpsys}' group by $column_values having count(1) > 1;"`
	if [[ $duplicate_count == '' ]]
	then
		L1_check_status=pass
		L1_Check_Insert
		loginfo "Data accuracy check successful. The MD5 checksum match."
	else
		L1_check_status=fail
		L1_Check_Insert
		loginfo "Data accuracy check failed. Duplicates found on the target . duplicated count: ${duplicate_count}."
	fi
}

#OLA table audit insertion functions
Ola_Audit_Insert()
{
    Ola_Day
	current_time=$(date +"%T")
	current_date=$(date '+%Y-%m-%d')
	if [[ $source_system == 'EDM T1' ]]
	then
		FILE_NAS_PATH='Not-Applicable'
		file_available_time='Not-Applicable'
		file_delayed='Not-Applicable'
	else
		file_available=$(stat -c %Y $FILE_PATH)
		file_available_time=$(date -d @$file_available +'%Y-%m-%d %H:%M:%S')
		loginfo "File available time for $file_name is $file_available_time"
		file_timestamp=$(stat -c '%y' "$FILE_PATH")
		file_timestamp_time=$(date -d "@$file_timestamp" "+%H:%M:%S")
		file_available_seconds=$(date -d "$file_timestamp_time" +%s)
		#file_available_unix=$(date -d "$file_timestamp_time"
		ola_seconds=$(date -d "$ola_time" +%s)
		elapsed_seconds=$((file_available_seconds - ola_seconds))
		file_delayed=$(date -u -d @"$elapsed_seconds" +%T)
	fi
	if [[ ${current_time} > ${ola_time} && ${ola_day} == ${current_date} ]]
	then
		ola_status="Breached"
	else
		ola_status="Not-Breached"
	fi
	insert_timestamp="$(date +%Y%m%d_%H%M%S)"
	Ola_Check_Insert
}

#L1 check audit insertion functions
L1_Check_Insert()
{
	cd ${FDP_DIR}/ingestion/scripts
	insert_timestamp="$(date +%Y%m%d_%H%M%S)"
	python3 fdp_L1_tracker.py $table_file_id $ods $country $tpsys $target_table $L1_check_type $L1_check_status $frequency $is_version_enabled $postgre_database $POSTGRES_USER $decrypted_password $postgre_host $postgre_port $l1_check_schema $l1_check_table $insert_timestamp
}

#OLA check audit insertion functions
Ola_Check_Insert()
{
    cd ${FDP_DIR}/ingestion/scripts
    python3 fdp_ola_tracker.py $table_file_id $ods $country $tpsys $target_table $FILE_NAS_PATH $file_name $ola_time "${file_available_time}" $ola_status $file_delayed $insert_timestamp $is_version_enabled $postgre_database $POSTGRES_USER $decrypted_password $postgre_host $postgre_port $ola_schema $ola_table $frequency
    loginfo "Audit insertion is completed for ola tracker table from python file"

}

#Get Dynamic partitions for table id
get_dynamic_partition()
{
  dynamic_partition_str=`cat ${FDP_DIR}/ingestion/param/dynamic_partition_string.cfg | awk -F '|' '$1==v2' v2=$table_id | cut -d '|' -f2`

}

#Roll over previous day data when ola is breached
OLA_breach_rollover()
{
  if [[ ${source_system} == 'HRS' ]]
  then
	target_schema_l=`echo ${target_schema} | tr '[:upper:]' '[:lower:]'`
	target_table_l=`echo ${target_table} | tr '[:upper:]' '[:lower:]'`
	previous_date=$(date --date="${ods} -1 day" +%Y-%m-%d)
	hdfs dfs -cp ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${previous_date} ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}
	`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "ALTER table ${target_schema_l}.${target_table_l} add if not exists partition (ods='${ods}',country='${country_upper}',tp_sys='${tpsys}') location '${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}/country=${country_upper}/tp_sys=${tpsys}'"`
	rollover_success=$?
	if [[ ${rollover_success} -eq 0 ]]
	then
		loginfo "Data rollover is successfull for the table ${target_schema_l}.${target_table_l} for ODS ${ods} from previous ODS ${previous_date}"
		exit 0
	else
		logerr "Data rollover has failed for the table ${target_schema_l}.${target_table_l} for ODS ${ods}. Please check."
	fi
  else
	loginfo "Data rollover is not required for OLA breach"
  fi
}



#Function to check ack for the financial contract table for fmrp
fmrp_recon()
{
 fmrp_recon_file_available='NO'
 recon_file_name=/CTRLFW/ADF/DF/COUNTRYEXTRACTS/FMRP/PRD/archive/FRDP_BLD_CHN_FINANCIAL_CONTRACT_${odsYYYYMMDD}.ack
 for ((i=1; i<=${no_of_retry}; i++));
  do
	if [ -f ${recon_file_name} ]
	then
		loginfo "Recon file for fmrp is available for ${recon_file_name}."
		fmrp_recon_file_available='YES'
		break
	else
		loginfo "Recon file for fmrp is not available in the file ${recon_file_name}. Hence, Audit entry will be check after 15 minutes"
		sleep 15m
	fi
  done

  if [[ ${fmrp_recon_file_available} == "YES" ]]
  then
      loginfo "Proceeding with loading as Recon ${recon_file_name} is available."
  else
      logerr "Recon file ${recon_file_name} is not available"
  fi
}

#Function to check ack for the write back tables for ebbs
recon_check()
{
 recon_file_available='NO'
 dependent_df_ack_file=`cat ${FDP_DIR}/ingestion/param/df_ack_file_mapper.txt | awk -F ':' '$1==v1' v1=${table_id} | cut -d ':' -f2`
 loginfo " The dependent_df_ack_file is ${dependent_df_ack_file}."
 recon_file_name=`echo ${dependent_df_ack_file} | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/YYYY-MM-DD/${ods}/g`
 for ((i=1; i<=${no_of_retry}; i++));
  do
	if [ -f ${recon_file_name} ]
	then
		loginfo "Recon file is available for ${recon_file_name}."
		recon_file_available='YES'
		break
	else
		loginfo "Recon file is not available in the file ${recon_file_name}. Hence, Audit entry will be check after 15 minutes"
		sleep 15m
	fi
  done

  if [[ ${recon_file_available} == "YES" ]]
  then
      loginfo "Proceeding with loading as Recon ${recon_file_name} is available."
  else
      logerr "Recon file ${recon_file_name} is not available"
  fi
}

#Function to check zero record count for BPSI Delta
Check_Bpsi_Delta_Total_Element_Count()
{
	bpsi_delta_total_element_count=`cat ${FILE_NAS_PATH}/${ods}/0*.json  |   python -c 'import json,sys;obj=json.load(sys.stdin);print obj["metaHeader"]["totalEventCount"]';`
	if [ $bpsi_delta_total_element_count == '0' ]
	then
		loginfo "The count is 0 for the date $ods. Hence, Skpipping the data load and validation in staging"
		exit 0
	else
		loginfo "The count is not 0 for the date $ods. Hence, proceeding with data loading"
	fi
}

#Function to check weekend for CREDX
Credx_Api_File_Check()
{
	credx_type=$(basename "$FILE_NAS_PATH" | tr '[:lower:]' '[:upper:]')
	if [ "$credx_type" = "EVENT" ] && { [ ${oday} == "Sat" ] || [ ${oday} == "Sun" ]; };
	then
		loginfo "$ods is a weekend. Hence, Skipping the data load and validation in staging"
		loginfo "CREDX type is $credx_type"
		exit 0
	else
		loginfo "$ods is not a weekend. Hence, proceeding with data loading"
	fi
}

generate_ack_file()
{
	ack_file_name=`echo ${target_table} | tr '[:lower:]' '[:upper:]'`
	unencrypted_hdfs_base_path=`echo ${hdfs_path} | sed -e "s/_enc//g"`
	hdfs_recon_path=${unencrypted_hdfs_base_path}/recon/${ods}/${tpsys_lower}
	ack_file_path=${hdfs_recon_path}/${ack_file_name}.txt
	echo "Unencrypted ACK HDFS File Name: ${ack_file_path}"
	ack_tmp_nas_path=${FDP_DIR}/temp/${ack_file_name}.txt

	table_count=`beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "select count(*) from ${target_schema}.${target_table} where ods='${ods}' and tp_sys='${tpsys}' and country='${country}';"`
	echo "Creating ACK File."
	echo -e "ods_country_tp_tablename|count\n${ods}_${country_upper}_${tpsys}_${ack_file_name}|${table_count}" > ${ack_tmp_nas_path}


	hdfs dfs -mkdir -p ${hdfs_recon_path}
	echo "Copying ACK File to HDFS."
	hdfs dfs -copyFromLocal -f ${ack_tmp_nas_path} ${ack_file_path}
	rm ${ack_tmp_nas_path}

}
#To handle blank files without header based on record count indicator
is_blank_file()
{
if [ ${record_count_at} == 'header' ]
then
	if [[ "$file_record_count_head" -eq 0 ]];
	then
		file_record_count="0"
		loginfo "count is zero.Hence skipping the data loading and exiting with 0"
		Call_Data_Rollover
	else
		file_record_count=$(echo $file_record_count_head | sed 's/^0*//')
		file_record_count=`echo ${file_record_count} | sed 's/[^0-9]*//g'`
	fi
else
	if [[ "$file_record_count_tail" -eq 0 ]];
	then
		file_record_count="0"
		loginfo "count is zero.Hence skipping the data loading and exiting with 0"
		Call_Data_Rollover
	else
		file_record_count=$(echo $file_record_count_tail | sed 's/^0*//')
		file_record_count=`echo ${file_record_count} | sed 's/[^0-9]*//g'`
	fi
fi
}

#To handle month end rollover for hrs and crp
Month_End_Rollover()
{
	day_of_week=$(date -d "$ods" +%u)
	last_working_day=""
	if [ "$day_of_week" -eq 6 ];
		then
			last_working_day=$(date --date="$ods -1 day" +%Y-%m-%d)
	elif [ "$day_of_week" -eq 7 ];
		then
			last_working_day=$(date --date="$ods -2 day" +%Y-%m-%d)
	else
		last_working_day=$ods
	fi
	last_working_day_YYYY_MM_DD=$(date --date="$last_working_day" +%Y-%m-%d)
	last_working_day_YYYYMMDD=$(date --date="$last_working_day" +%Y%m%d)
	last_working_day=`echo ${file_name} | sed -e s/YYYY-MM-DD/${last_working_day_YYYY_MM_DD}/g | sed -e s/YYYYMMDD/${last_working_day_YYYYMMDD}/g`
	for ((i=1; i<=${no_of_retry}; i++))
	do
		if [ -f ${FILE_NAS_PATH}/$last_working_day ]
			then
				loginfo "Previous working day file $last_working_day is available. Hence, copying the files with current ODS date"
				converted_file_name=`echo ${file_name} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`
				conv_file=${FILE_NAS_PATH}/$converted_file_name
				ola_mail_triggered='NO'
				cp ${FILE_NAS_PATH}/$last_working_day ${conv_file}
				Upload_File_To_HDFS
				break
		else
			loginfo "file is not available. Hence, waiting for 15 min"
		fi
		sleep 15m
	done
}

Loading_File_Path()
{
	me_date=$(date --date "$(date --date $ods +%Y-%m-01) +1 month -1 day" +%Y-%m-%d)
	echo $me_date
	if [[ "$ods" == "$me_date" && ${tpsys} == "BCRS" ]]
	then
		FILE_PATH=`echo ${FILE_PATH1} | sed -e s/YYYY-MM-DDPREVMONTHDATE/${previous_month_end}/g | sed -e s/YYYYMMDDPREVMONTHDATE/${previous_month_end}/g`
	else
		FILE_PATH=`echo ${FILE_PATH1} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`
	fi
}
#Function to do data rollover from previous date to current ods
Data_Rollover()
{
  target_schema_l=`echo ${target_schema} | tr '[:upper:]' '[:lower:]'`
  target_table_l=`echo ${target_table} | tr '[:upper:]' '[:lower:]'`
  previous_date=$(date --date="${ods} -1 day" +%Y-%m-%d)
  hdfs dfs -rm -r ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}/country=${country}/tp_sys=${tpsys}
  hdfs dfs -mkdir -p ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}/country=${country}/tp_sys=${tpsys}
  hdfs dfs -cp ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${previous_date}/country=${country}/tp_sys=${tpsys} ${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}/country=${country}/
  `beeline -u ${BEELINE_STR} --silent=false --showHeader=false --outputformat=tsv2 -e "ALTER table ${target_schema_l}.${target_table_l} add if not exists partition (ods='${ods}',country='${country}',tp_sys='${tpsys}') location '${hdfs_path}/${target_schema_l}/${target_table_l}/ods=${ods}/country=${country}/tp_sys=${tpsys}'"`
  rollover_success=$?
  if [[ ${rollover_success} -eq 0 ]]
  then
    loginfo "Data rollover is successfull for the table ${target_schema_l}.${target_table_l} for ODS ${ods} from previous ODS ${previous_date}"
	 exit 0
  else
    logerr "Data rollover has failed for the table ${target_schema_l}.${target_table_l} for ODS ${ods}. Please check."
  fi
}
#Function to call data rollover functions
Call_Data_Rollover()
{
	if [[ ${is_delta_load} == 'Y' ]];
	then
		loginfo "count is zero and snapshot is required, Hence doing rollover from previous day"
		Data_Rollover
	else
		loginfo "count is zero and snapshot not required, Hence skipping the data loading and exiting with 0"
		exit 0
	fi
}	

#Function to have integrated sql for EDM T1
Integrated_Query()
{
  loginfo "Checking the Integrated recon table for the table id ${table_id}"
  tp=`echo ${source_table} | cut -d '_' -f1`
  country=`echo ${source_table} | cut -d '_' -f2`
  actual_table_name=`echo ${source_table} | cut -d '_' -f3-`
  recon_db=${tp}_prd_ops
  recon_table=${tp}_${country}_integrated_recon
  if [[ ${environment_lower} == "prd" ]]
  then
      recon_db=${recon_db}
  else
      recon_db=${exception_db_prefix}_${recon_db}
  fi
  t1_audit_sql="select if(overall_recon_report in ('MATCH','MISMATCH'),'Yes','No') as status from ${recon_db}.${recon_table} where partition_date='${ods}' and upper(table_name) =UPPER('${actual_table_name}') limit 1 ;"
  loginfo "Running the below SQL to check the status of Audit entry \n \t $t1_audit_sql"
}


#Function to check the availability of PSGL ACK files
psgl_ack_watcher()
{
  ack_file=${FDP_DIR}/data/psgl_ack/${ods}/psgl_${country}_${ods}.txt
  psgl_ack_available="NO"
  for ((i=1; i<=${no_of_retry}; i++));
  do
    if [ -e ${ack_file} ]
    then
        loginfo "ACK file ${ack_file} is available. Hence, proceeding with loading."
        psgl_ack_available="YES"
        break
    else
        loginfo "ACK file ${ack_file} is not available. Hence,a recheck will be done in 15 minutes."
        sleep 15m
    fi
    done

    if [[ ${psgl_ack_available} == "YES" ]]
    then
        loginfo "Proceeding with PSGL ingestion as ACK file is available"
    else
        logerr "ACK file ${ack_file} is not available after ${no_of_retry}. Hence, failing the job."
    fi

}

Biannual_run()
{
	current_month=$(date +%m)
	last_day_of_month=$(date --date "$(date --date $ods +%Y-%m-01) +1 month -1 day" +%Y-%m-%d)
	new_filename=${file_name/_DDMMYYYY*.*/}
	if [[ ${ods} == ${last_day_of_month} && ( ${current_month} == "05" || ${current_month} == "11" ) ]]
	then
		loginfo "Mosaic files need to be loaded today as part of bi annual Run"
		latest_available_file=$(ls -tr $FILE_NAS_PATH | grep -i $new_filename'\_[0-9]\{1,8\}\.dat.gz'  | tail -1)
		converted_file_name=`echo ${file_name} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`
		conv_file=${FILE_NAS_PATH}/$converted_file_name
		ola_mail_triggered='NO'
		cp ${FILE_NAS_PATH}/$latest_available_file ${conv_file}
		Upload_File_To_HDFS
	else
		loginfo "Biannual_run is not required to run"
	fi
}

#Updating schema value for count validation
Update_Schema_For_Count_Check()
{
	intermediate_stage_load=$(echo "$intermediate_stage_load" | tr '[:lower:]' '[:upper:]')
	non_prod_staging_load=$(echo "$non_prod_staging_load" | tr '[:lower:]' '[:upper:]')
	intermediate_source_schema_name=$(echo "$intermediate_source_schema_name" | tr '[:lower:]' '[:upper:]')

	if [[ "$intermediate_stage_load" == "Y" && "$non_prod_staging_load" == "Y" ]]; 
	then
		if [[ "${target_schema,,}" == ${intermediate_app_replaceable_key,,}* ]]; 
		then
    			intermediate_staging_schema=$(echo "$target_schema" | sed "s/^${intermediate_app_replaceable_key}/${intermediate_app_replaceable_value}/I")
  		else
    			intermediate_staging_schema=$target_schema
  		fi
		source_schema="${intermediate_source_schema_name}"		  
  		target_schema="${intermediate_staging_schema}"
		target_table="${source_table}"
	elif [[ "$intermediate_stage_load" == "N" && "$non_prod_staging_load" == "Y" ]]; 
	then
  		if [[ "${target_schema,,}" == ${intermediate_app_replaceable_key,,}* ]]; 
		then
    			source_schema=$(echo "$target_schema" | sed "s/^${intermediate_app_replaceable_key}/${intermediate_app_replaceable_value}/I")
		else
    			source_schema="${source_schema}"
  		fi
	target_schema="${target_schema}"
	else
		target_schema="${target_schema}"			
	fi
}

#Checksum check for EBBS and CMS 
ebbs_cms_rco_checksum()
{
	for files in `ls $FILE_PATH`
	do
		updated_file_name=`basename ${files} .gz`
		if [[ ${tpsys} == 'EBBS'  ]];
		then
			zcat ${FILE_PATH} > ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}
			file_name=`basename ${FDP_DIR}/ingestion/preprocessing/${updated_file_name}`
			loginfo "file_name is ${file_name}"
			cat ${FDP_DIR}/ingestion/preprocessing/${updated_file_name} | sed '1d;$d' | sed '$d' | sed ':a;N;$!ba;s/\n//g' > ${FDP_DIR}/ingestion/preprocessing/${file_name}_checksum
			truncate -s-1 ${FDP_DIR}/ingestion/preprocessing/${file_name}_checksum
			file_checksum=$(sha256sum ${FDP_DIR}/ingestion/preprocessing/${file_name}_checksum | cut -f1 -d\ )
			loginfo "file checksum generated from the file ${file_name} is ${file_checksum}"
			read_control_file=$(zcat ${FILE_PATH} | tail -n 1)
			loginfo "checksum from control file from the file ${FILE_PATH} is ${read_control_file}"
			rm -f ${FDP_DIR}/ingestion/preprocessing/${file_name}_checksum
			loginfo "Removing the temp file ${FDP_DIR}/ingestion/preprocessing/${file_name}_checksum from preprocessing after generating checksum."
			L1_Checksum_Entry
		elif [[ ${tpsys} == 'CMS'  ]];
		then
			file_checksum=$(zcat ${FILE_PATH} | sed '$d' | sha256sum | awk '{print $1}')
			loginfo "file checksum generated from the file ${file_name} is ${file_checksum}"
			file_checksum_upper=${file_checksum^^}
			control_file=$(zcat ${FILE_PATH} | tail -n 1)
			control_file_value=$(echo "${control_file}" | cut -d'Â§' -f2)
			read_control_file=$(echo "$control_file_value" | tr -d '\r$')
			loginfo "checksum from control file from the file ${FILE_PATH} is ${read_control_file}"
			L1_Checksum_Entry
		elif [[ ${tpsys} == 'RCO' ]];
		then
			file_checksum=$(sha256sum ${FILE_PATH} | awk '{print $1}')
			loginfo "file checksum generated from the file ${file_name} is ${file_checksum}"
			checksum_path_from_metadata=$(echo "$file_checksum_path" | cut -d';' -f1)
			checksum_path=`echo ${checksum_path_from_metadata} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`		
			checksum_path_defined=${FILE_NAS_PATH}/${checksum_path}
			checksum_path=`echo ${checksum_path_defined} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`
			read_control_file=$(zcat ${checksum_path} | awk -F"$delimiter" '{for (i=1; i<=NF; i++) if(length($i) >60) print $i}' | cut -d'|' -f2)
			loginfo "control file name is $read_control_file"
			L1_Checksum_Entry
		else
			loginfo "special case checkum is not applicable"
		fi
	done
}

#To implement checksum based on control file provided by sourcing team
is_checksum_sha256()
{
	if [[ ${file_checksum_path} != '' ]];
	then
		checksum_path_from_metadata=$(echo "$file_checksum_path" | cut -d';' -f1)
		checksum_path=`echo ${checksum_path_from_metadata} | sed -e s/YYYY-MM-DD/${ods}/g | sed -e s/YYYYMMDD/${odsYYYYMMDD}/g | sed -e s/DDMMYYYY/${odsDDMMYYYY}/g  | sed -e s/YYYY/${oyear}/g | sed -e s/DDMMYY/${odsDDMMYY}/g`		
		checksum_path_defined=${FILE_NAS_PATH}/${checksum_path}
		if [[ ${checksum_path} == ${FILE_PATH} ]];
		then
			read_control_file=$(zcat ${FILE_PATH} | tail -n 1)
			loginfo "control file name is $read_control_file"
		elif [[ ${checksum_path##*.} == gz ]];
		then	
			control_file_value=$(zcat ${checksum_path_defined} | awk -F"$delimiter" '{for (i=1; i<=NF; i++) if(length($i) >60) print $i}' | cut -d'|' -f2)
			read_control_file=$(echo "$control_file_value" | tr -d '\r$')
			loginfo "control file name is $read_control_file"
		else
			control_file_value=$(awk -F"$delimiter" '{for (i=1; i<=NF; i++) if(length($i) >40) print $i}' ${checksum_path_defined} | cut -d'|' -f2)
			read_control_file=$(echo "$control_file_value" | tr -d '\r$')
			loginfo "control file name is $read_control_file"
		fi
	else
		loginfo " file checksum is not defined hence ignoring checksum check"
		read_control_file=""
	fi

	if [[ ${FILE_PATH##*.} == gz && ${checksum_path_from_metadata} == '' ]];
	then
		file_checksum=""
		loginfo " file checksum is not defined hence ignoring checksum check"
	elif [[ ${FILE_PATH##*.} == gz ]];
	then
		file_checksum=$(gzip -dc ${FILE_PATH} | sha256sum | awk '{print $1}')
		loginfo "File checksum generated is $file_checksum"
	else
		file_checksum=$(sha256sum ${FILE_PATH} | awk '{print $1}')
		loginfo "File checksum generated is $file_checksum"
	fi
}

#L1 audit entry for checksum
L1_Checksum_Entry()
{
    L1_check_type=file_checksum_validation
	if [[ ${read_control_file^^} == ${file_checksum^^} ]];
	then
		loginfo "file checksum generated based on sha 256 algorithm and after reading control file is matching"
		L1_check_status=pass
		L1_Check_Insert
	else
		L1_check_status=fail
		L1_Check_Insert
		loginfo "file checksum generated based on sha 256 algorithm and after reading control file is not matching"
	fi
}

#To call checksum functions
Call_Checksum()
{
	if [[ ${is_checksum_enabled^^} == 'Y' ]];
	then
		loginfo "checksum is enabled"
		calling_function=$(echo "$file_checksum_path" | cut -d';' -f2)
		loginfo "Calling the function $calling_function"
		if [[ ${calling_function^^} == 'EBBS_CMS_RCO_CHECKSUM' ]];
		then 
			loginfo "Calling the function EBBS_CMS_RCO_CHECKSUM"
			ebbs_cms_rco_checksum
			L1_Checksum_Entry
		else
			loginfo "Calling the function is_checksum_sha256"
			is_checksum_sha256
			L1_Checksum_Entry
		fi
	else
		loginfo "checksum is not enabled"
	fi	
}


