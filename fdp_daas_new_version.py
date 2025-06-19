"""
1. Read the config params ( env & env_name and other params)
2. Read the json file
3. Execute the SQ
"""
from pyspark.sql.functions import col
from collections import OrderedDict
import json
import pyspark.sql
import pandas as pd
import pyspark.sql.functions as F
import os
import sys
import glob
import re
import datetime as dt
from datetime import timedelta
import subprocess as sp
import logging
import zipfile
import shutil
import configparser
from pyspark.sql.types import NullType
from sparksession import get_spark_session
import argparse
import datetime
from pyspark.sql.functions import lit
import psycopg2
import db_connect
import getlogger
from sql_metadata import Parser
from psycopg2 import OperationalError
from psycopg2 import InterfaceError
from psycopg2 import InternalError
import csv

closing_cursor = "Closing the cursor.."
creating_cursor = "Creating the cursor.."
in_param = "in ('${<"
lower_version = ">_version}')"
less_than = " ('${<"
in_condition = "in ('"
IN_CONDITION = "IN ('"
date_format = "%Y-%m-%d-%H%M%S"
version = ">_version}"
part_col = "part col "
beelinestr = "beeline -u '"
silent_otpt_format = "' --silent=true --outputformat=csv2 -e '"
cmd_to_trigger = "Command to trigger:{}"
ods_val = " ods_val='"
freq_val = "' and frequency_val='"
prodver_query = "Query for fetching product version is below:"
maxver_query = "select max(version_val) as version_val from "
where = " where"
where_ods = " where ods='"
lower_target_table = " and LOWER(target_table_name)='"
freq = " and frequency='"
calling_var_postgres = "Calling initialize_var_for_postgres_and_insert function"
equal_zero = "='{0}'"
internal_job_run_status = "FAILED"
internal_exception_details = "Pyscopg2 Internal Error Occured"
internal_rowcount = "NA"


def process_ddl():
    metadata_df = get_active_services_metadata_df()
    active_services = list(metadata_df['Service_Name'])
    logger.info("Metadata columns:{}".format(metadata_df.columns))
    global table_name
    table_name = metadata_df['PRODUCT'][0]
    global db_name
    db_name = get_db_name()
    check_and_create_db_if_not_exists(db_name)
    service_to_col_datatype_dict = {}
    for service in active_services:
        service_df = metadata_df[metadata_df['Service_Name'] == service]
        col_data_type = service_df[['ParentAttribute', 'AttributeDataType']].values
        metadata_col_to_datatype_dict = OrderedDict()
        for c_d in col_data_type:
            metadata_col_to_datatype_dict[c_d[0]] = c_d[1]
        service_to_col_datatype_dict[service] = metadata_col_to_datatype_dict
    # validate_col_datatypes_in_all_services(service_to_col_datatype_dict, active_services)
    first_service = active_services[0]
    metadata_col_to_datatype_dict = service_to_col_datatype_dict[first_service]
    if (is_table_present(table_name, db_name)):
        compare_and_generate_alter_stmts(metadata_col_to_datatype_dict)
    else:
        create_query = "CREATE EXTERNAL TABLE {0}.{1} (".format(db_name, table_name)
        for k, v in metadata_col_to_datatype_dict.items():
            if v.strip().lower() == 'integer':
                create_query += "{0} {1},".format(k, 'int')
            else:
                create_query += "{0} {1},".format(k, v)
        create_query = create_query.rstrip(',')
        create_query += " ) PARTITIONED BY (ods_val string, country_val string, tp_sys_val string, process_id_val string) STORED AS PARQUETFILE "
        create_query += " LOCATION '{0}'".format(get_table_path_from_db_tb(db_name, table_name))
        logger.info("create query ==========> " + create_query)
        cmd = beelinestr + beeline_string + silent_otpt_format + create_query + "'"
        logger.info(cmd_to_trigger.format(cmd))
        op, err = sp_execute(cmd)
        if len(err) > 0:
            logger.info("Exception while creating table and details as follows:{}".format(err))

def varchar_and_string_dttypes(col):
    logger.info("varchar and string is matching for {}".format(col))

def int_dattype(col):
    logger.info("Int and Integer type is matching for {}".format(col))


def alt_log_info(meta_col_data_dict,td_dict,col):
    logger.info("Mismatch {0}   metadata datatype {1} table datatype {2}".format(col, meta_col_data_dict[col],
                                                                                 td_dict[col]))
    if meta_col_data_dict[col].lower() == 'integer':
        alter_stmt = "ALTER TABLE {0}.{1} CHANGE {2} {3} {4}".format(db_name, table_name, col, col, "int")
    else:
        alter_stmt = "ALTER TABLE {0}.{1} CHANGE {2} {3} {4}".format(db_name, table_name, col, col,meta_col_data_dict[col])
    logger.info("DATA TYPE CHANGE: " + alter_stmt)
    cmd = beelinestr + beeline_string + silent_otpt_format + alter_stmt + "'"
    logger.info(cmd_to_trigger.format(cmd))
    op, err = sp_execute(cmd)
    if len(err) > 0:
        logger.info("Exception while altering the  table and details as follows:{}".format(err))

def alt_tbl_dtls(meta_col_data_dict,col):
    logger.info("{0}  is missing in table".format(col.lower()))
    add_column_stmt = "ALTER TABLE {0}.{1} ADD COLUMN({2} {3})".format(db_name, table_name, col, meta_col_data_dict[col])
    cmd = beelinestr + beeline_string + silent_otpt_format + add_column_stmt + "'"
    logger.info(cmd_to_trigger.format(cmd))
    op, err = sp_execute(cmd)
    if len(err) > 0:
        logger.info("Exception while altering the  table and details as follows:{}".format(err))


def compare_and_generate_alter_stmts(metadata_col_to_datatype_dict):
    td_dict = get_existing_table_col_datatypes_dict()
    meta_col_data_dict = {k.lower(): v.lower() for k, v in metadata_col_to_datatype_dict.items()}
    td_dict = {k.lower(): v.lower() for k, v in td_dict.items()}
    for col in meta_col_data_dict.keys():
        if col in td_dict:
            if (meta_col_data_dict[col] != td_dict[col]):
                if (is_varchar_and_string_datatypes(meta_col_data_dict, td_dict, col)):
                    varchar_and_string_dttypes(col)
                elif (int_and_integer_datatypes(meta_col_data_dict, td_dict, col)):
                    int_dattype(col)
                else:
                    alt_log_info(meta_col_data_dict,td_dict,col)
            else:
                logger.info(col + " is matching ")
        else:
            alt_tbl_dtls(meta_col_data_dict,col)


def sp_execute(cmd):
    return "True", ""


def is_varchar_and_string_datatypes(meta_col_data_dict, td_dict, col):
    return (meta_col_data_dict[col].startswith("varchar") and td_dict[col].startswith("string")) or (
                meta_col_data_dict[col].startswith("string") and td_dict[col].startswith("varchar"))


def int_and_integer_datatypes(meta_col_data_dict, td_dict, col):
    return (meta_col_data_dict[col].lower().startswith("integer") and td_dict[col].lower().startswith("int")) or (
            meta_col_data_dict[col].lower().startswith("int") and td_dict[col].lower().startswith("integer"))


def get_existing_table_col_datatypes_dict():
    dsc = spark.sql("select * from {0}.{1} limit 1".format(db_name, table_name))
    td = dsc.dtypes
    td_dict = {}
    for tde in td:
        td_dict[tde.__getitem__(0)] = tde.__getitem__(1)
    return td_dict


def validate_col_datatypes_in_all_services(service_to_col_datatype_dict):
    services = service_to_col_datatype_dict.keys()
    services = list(services)
    match_flag = True
    first_service_dict = service_to_col_datatype_dict[services[0]]
    if (len(services) > 0):
        for service in services:
            match_flag = match_flag and (first_service_dict == service_to_col_datatype_dict[service])
    logger.info("validator status " + str(match_flag))
    return match_flag


def get_active_services_metadata_df():
    metadata_file_path = get_current_metadata_path()
    metadata_df = pd.read_csv(metadata_file_path, skiprows = 1)
    return metadata_df


def get_current_metadata_path():
    return get_config_value("metadata_upload_path") + "/" + "DP_" + args.product_name_upper + "_Metadata.csv"


def is_table_present(table_name, target_schema):
    table_exception_msg = ""
    try:
        dsc = spark.sql("select * from {0}.{1} limit 1".format(target_schema, table_name))
    except Exception as e:
        table_exception_msg = "got exception"
        logger.info(table_exception_msg)
    return len(table_exception_msg) == 0


def get_latest_version(version_str):
    major = int(version_str.split("V")[1])
    number_part = str(major + 1)
    if int(number_part) <= 9:
        new_version_str = "V" + "0" + number_part
    else:
        new_version_str = "V" + number_part
    return new_version_str


def fetch_current_version(part_info_for_registering_partition, db, table,table_path,path_with_partition_info_only):
    logger.info("Table path:{}".format(table_path))
    where_clause = " and ".join(part_info_for_registering_partition) + " and frequency_val='" + args.frequency + "'"
    logger.info("Where clause:{}".format(where_clause))
    logger.info("Part info for registering partition:{}".format(part_info_for_registering_partition))
    logger.info("Path with partition info:{}".format(path_with_partition_info_only))
    path_to_check=table_path+"/"+path_with_partition_info_only
    version_string=get_version_string(path_to_check)
    logger.info("Version string is :{}".format(version_string))
    return version_string
    #dfv = spark.sql("select max(version_val) as version_val from " + db + "." + table + " where " + where_clause)
    #version_string = [row.version_val for row in dfv.select("version_val").collect()][0]
    #if version_string is None:
        #return "NA"
    #else:
        #return version_string

def get_timestamp_version(version_str):
    timestamp_str = dt.datetime.now().strftime("%d%m%Y_%H_%M_%S_%f")[:-3]
    return version_str + "_" + timestamp_str


def create_cursor(conn):
    conn = create_postgre_connection()
    cur = conn.cursor()
    return cur

def close_cursor(cur):
    cur.close()


def fetch_max_version_product(table):
    where_clause = ods_val + args.ods + freq_val + args.frequency + "'"
    logger.info(prodver_query)
    logger.info(maxver_query + table + where + where_clause)
    dfv = spark.sql(maxver_query + table + where + where_clause)
    if len(dfv.select("version_val").collect()) == 0:
        version_string = "NA"
    else:
        version_string = [row.version_val for row in dfv.select("version_val").collect()][0]
    return version_string

def fetch_max_version_product_tp(table):
    where_clause = ods_val + args.ods + freq_val + args.frequency + "' and LOWER(tp_sys_val)='" + part_col_to_val_or_var_map['tp_sys_val'].lower() + "'"
    logger.info(prodver_query)
    logger.info(maxver_query + table + where + where_clause)
    dfv = spark.sql(maxver_query + table + where + where_clause)
    if len(dfv.select("version_val").collect()) == 0:
        version_string = "NA"
    else:
        version_string = [row.version_val for row in dfv.select("version_val").collect()][0]
    return version_string

def fetch_version_for_current_run(version_string):
    if version_string == 'NA':
        interim_version = 'V00'
        final_version = get_timestamp_version(interim_version)
    else:
        interim_version = get_latest_version(version_string.split("_")[0])
        final_version = get_timestamp_version(interim_version)
    return final_version


def fetch_max_version(cur, schema, table, i, connection):
    try:
        cur.execute(
            "select max(version) from " + schema + "." + table + where_ods + args.ods + "'" + lower_target_table + i.lower() + "'" + freq + args.frequency + "'")
        max_version = cur.fetchone()[0]
        logger.info("the frequency is {} ,the schema is {} ,the table is {} and target table is {}".format(args.frequency, schema, table, i))
        logger.info("Max version for table {} is {}".format(i, max_version))
        return max_version
    except InterfaceError as e:
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        connection=check_interface_error_and_return_connection(connection,cur,e)
        logger.info("Calling fetch_max_version function")
        return fetch_max_version(cur, schema, table, i, connection)
    except OperationalError as e:
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        connection=check_operational_error_and_return_connection(connection,cur,e)
        logger.info("Calling fetch_max_version function")
        return fetch_max_version(cur, schema, table, i, connection)
    except InternalError as e:
        cursor_con=create_cursor(connection)
        connection = check_operational_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(internal_job_run_status, internal_exception_details, internal_rowcount, connection)

def fetch_max_version_tp(cur, schema, table, i, connection):
    try:
        cur.execute(
            "select max(version) from " + schema + "." + table + where_ods + args.ods + "'" + lower_target_table + i.lower() + "'" + freq + args.frequency + "'"
            + " and LOWER(tp)='"+part_col_to_val_or_var_map['tp_sys_val'].lower()+"'")
        logger.info("Generating max version for TP:{}".format(part_col_to_val_or_var_map['tp_sys_val']))
        max_version = cur.fetchone()[0]
        logger.info(
            "the frequency is {} ,the schema is {} ,the table is {} and target table is {}".format(args.frequency, schema,
                                                                                                   table, i))
        logger.info("Max version for table {} is {}".format(i, max_version))
        return max_version
    except InterfaceError as e:
        connection = check_interface_error_and_return_connection(connection, cur, e)
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        logger.info("Calling fetch_max_version_tp function")
        return fetch_max_version_tp(cur, schema, table, i, connection)
    except OperationalError as e:
        connection = check_operational_error_and_return_connection(connection, cur, e)
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        logger.info("Calling fetch_max_version_tp function")
        return fetch_max_version_tp(cur, schema, table, i, connection)
    except InternalError as e:
        cursor_con=create_cursor(connection)
        connection = check_operational_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(internal_job_run_status, internal_exception_details, internal_rowcount,
                                               connection)


def fetch_target_table_list(cur, schema, table, connection):
    try:
        cur.execute(
            "select distinct target_table_name from " + schema + "." + table + where_ods + args.ods + "'" + freq + args.frequency + "'")
        auditinterimlist = cur.fetchall()
        audittablelist = [x[0] for x in auditinterimlist]
        return audittablelist
    except InterfaceError as e:
        connection = check_interface_error_and_return_connection(connection, cur, e)
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        logger.info("Calling fetch_target_table_list function")
        return fetch_target_table_list(cur, schema, table, connection)
    except OperationalError as e:
        connection = check_operational_error_and_return_connection(connection, cur, e)
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        logger.info("Calling fetch_target_table_list function")
        return fetch_target_table_list(cur, schema, table, connection)
    except InternalError as e:
        cursor_con=create_cursor(connection)
        connection = check_operational_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(internal_job_run_status, internal_exception_details, internal_rowcount,
                                               connection)


def get_table_names(query):
    parsed_query = Parser(query)
    table_names = parsed_query.tables
    return table_names

def create_hdfs_dir(path):
    cmd = "hdfs dfs -mkdir -p " + path
    logger.info("Creating hdfs folder:{}".format(cmd))
    proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    proc.communicate()

def get_version_string(path_to_check):
    cmd = "hdfs dfs -ls -t " + path_to_check + ' | grep "^d" | head -n 1 |' + " awk '{print $NF}' | grep '/[^/]*$' | sed 's/.*\///'"
    proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    op, err = proc.communicate()
    op=op.decode("utf-8")
    if len(op)!=0:
        version_string=op.split("\n")[0].split("=")[1]
    else:
        version_string="NA"
    return version_string

def create_extra_countries_recon(extra_countries):
    logger.info("Iterating the extra countries list")
    for i in extra_countries:
        logger.info("Writing recon file for country {}".format(i))
        write_to_recon(i, 0)

def active_containers(product_name):
    logger.info("No active containers are available for product {}".format(product_name))
    sys.exit(0)

def audit_entries():
    logger.info("Starting the processing...")
    if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'postgre':
        initialize_var_for_postgres_and_insert("STARTED", "NA", "NA", connection)
    if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'hive':
        initialize_var_for_hive_and_insert("STARTED", "NA", "NA")


def audit_entries_1():
    if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'postgre':
        initialize_var_for_postgres_and_insert("RUNNING", "NA", str(num_rows_in_df), connection)
    if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'hive':
        initialize_var_for_hive_and_insert("RUNNING", "NA", str(num_rows_in_df))

def tp_sys(distinct_dynamic_partitions_vals_arr):
    if (part_col_to_val_or_var_map['tp_sys_val']=='GPTM') or (part_col_to_val_or_var_map['tp_sys_val']=='SFMP') or (part_col_to_val_or_var_map['tp_sys_val']=='ACBS') or (part_col_to_val_or_var_map['tp_sys_val']=='S2BL') or (part_col_to_val_or_var_map['tp_sys_val'] == 'DTP') or (part_col_to_val_or_var_map['tp_sys_val'] == 'FPSL' and part_col_to_val_or_var_map['process_id_val'] == 'SUB_LDGR-FPSL-GPTM-ACTG') or (part_col_to_val_or_var_map['process_id_val'] == 'ACTVT_LMT-LTP-ACTVTLMT') or (part_col_to_val_or_var_map['process_id_val'] == 'CLTRL_PERFECTED-COCOADTL') or (part_col_to_val_or_var_map['tp_sys_val'] == 'OTP') :
        distinct_dynamic_parts_list=[row.CTRY_CD for row in distinct_dynamic_partitions_vals_arr]
        extra_countries = list(set(country_str) - set(distinct_dynamic_parts_list))
        if len(extra_countries) > 0:
            logger.info("Extra countries found:{}".format(extra_countries))
            logger.info("Creating recon file for extra countries")
            create_extra_countries_recon(extra_countries)
    else:
        logger.info("Extra countries check not required for non GPTM/ACBS/SFMP/FPSL/DTP/LTP/COCOA/OTP TP systems")

def partition_col(is_write_required,table_version_path,table_path,dynamic_parts_dict,dynamic_part_col_vals,part_info_for_registering_partition,path_with_partition_info_only,prod_container_df,db_name,filtered_df,target_table):
    for part_col_name in part_col_to_val_or_var_map.keys():
        if is_dynamic_partition_col(part_col_name, dynamic_parts_dict):
            logger.info("Part_col_name {} is  a dynamic part".format(part_col_name))
            dynamic_part_val = str(dynamic_part_col_vals.__getattr__(part_col_to_val_or_var_map[part_col_name]))
            logger.info("Dynamic part val is {}".format(dynamic_part_val))
            if (is_dynamic_val_equal_to_filter(dynamic_part_val)):
                logger.info("{0} is equivalent to filter".format(dynamic_part_val))
                path_with_partition_info_only += part_col_name + "=" + dynamic_part_val + "/"
                logger.info("Filtering the {0}={1}".format(part_col_to_val_or_var_map[part_col_name],
                                                           dynamic_part_val))
                filtered_df = filtered_df.filter(
                    "{0}='{1}'".format(part_col_to_val_or_var_map[part_col_name], dynamic_part_val))
                logger.info("Number of rows for the filtered df as per the current filter: " + str(
                    filtered_df.count()))
                part_info_for_registering_partition.append(
                    part_col_name + equal_zero.format(dynamic_part_val))
                logger.info("Path with partition info only:{}".format(path_with_partition_info_only))
                logger.info("part_info_for_registering_partition:{}".format(part_info_for_registering_partition))
            else:
                logger.info("{0} is not equivalent to filter".format(dynamic_part_val))
                is_write_required = False

        else:
            logger.info("Part_col_name {} is not a dynamic part".format(part_col_name))
            path_with_partition_info_only += part_col_name + "=" + part_col_to_val_or_var_map[
                part_col_name] + "/"
            part_info_for_registering_partition.append(
                part_col_name + equal_zero.format(part_col_to_val_or_var_map[part_col_name]))
    logger.info("Partition clause for one distinct set:{}".format(path_with_partition_info_only))
    if is_write_required:
        logger.info("Partition clause for one distinct set:{}".format(path_with_partition_info_only))
        if version_enabled == 'Y':
            old_version_partition_path = path_with_partition_info_only + "frequency_val" + "=" + args.frequency + "/"
            partition_path_till_frequency = old_version_partition_path
            logger.info("Partition path till frequency:{}".format(partition_path_till_frequency))
            path_with_partition_info_only += "frequency_val" + "=" + args.frequency + "/"
            part_info_for_registering_partition.append("frequency_val" + equal_zero.format(args.frequency))
            part_info_for_registering_partition_till_frequency = part_info_for_registering_partition
            version_string = fetch_current_version(part_info_for_registering_partition, db_name,
                                                   get_target_table_name(prod_container_df),table_path,path_with_partition_info_only)
            new_version = fetch_version_for_current_run(version_string)
            old_part_info_for_registering_partition = [x for x in part_info_for_registering_partition]
            if not new_version.startswith('V00'):
                logger.info("Old version {0} exists ".format(version_string))
                old_version_partition_path+="version_val" + "=" + version_string + "/"
                old_part_info_for_registering_partition.append(
                    "version_val" + equal_zero.format(version_string))
            path_with_partition_info_only += "version_val" + "=" + new_version + "/"
            part_info_for_registering_partition.append("version_val" + equal_zero.format(new_version))
            logger.info("Final Path with partition info only:{}".format(path_with_partition_info_only))
            logger.info("Final part_info_for_registering_partition:{}".format(part_info_for_registering_partition))
            logger.info("Old version partition path:{}".format(old_version_partition_path))
            logger.info("Old path info partition:{}".format(old_part_info_for_registering_partition))
        else:
            partition_path_till_frequency = ""
            part_info_for_registering_partition_till_frequency = []
        write_to_target_dir(filtered_df, path_with_partition_info_only, table_path,
                            part_info_for_registering_partition,table_version_path,old_version_partition_path,old_part_info_for_registering_partition)
        check_and_handle_more_versions(partition_path_till_frequency,
                                       part_info_for_registering_partition_till_frequency, new_version, db_name,
                                       target_table, table_path)
        check_and_handle_extra_partitions(new_version, part_info_for_registering_partition_till_frequency,
                                          db_name, target_table)
    else:
        logger.info("Skipping as filter is not applicable")


def old_version_parition(table_version_path,path_with_partition_info_only,part_info_for_registering_partition,prod_container_df,table_path,filtered_df,db_name,target_table):
    if version_enabled == 'Y':
        old_version_partition_path = path_with_partition_info_only + "frequency_val" + "=" + args.frequency + "/"
        partition_path_till_frequency = old_version_partition_path
        path_with_partition_info_only += "frequency_val" + "=" + args.frequency + "/"
        part_info_for_registering_partition.append("frequency_val" + equal_zero.format(args.frequency))
        part_info_for_registering_partition_till_frequency = part_info_for_registering_partition
        version_string = fetch_current_version(part_info_for_registering_partition, db_name,
                                               get_target_table_name(prod_container_df),table_path,path_with_partition_info_only)
        new_version = fetch_version_for_current_run(version_string)
        old_part_info_for_registering_partition = [x for x in part_info_for_registering_partition]
        if not new_version.startswith('V00'):
            logger.info("Old version {0} exists ".format(version_string))
            old_version_partition_path += "version_val" + "=" + version_string + "/"
            old_part_info_for_registering_partition.append(
                "version_val" + equal_zero.format(version_string))
        path_with_partition_info_only += "version_val" + "=" + new_version + "/"
        part_info_for_registering_partition.append("version_val" + equal_zero.format(new_version))
        logger.info("Final Path with partition info only:{}".format(path_with_partition_info_only))
        logger.info("Final part_info_for_registering_partition:{}".format(part_info_for_registering_partition))
        logger.info("Old version partition path:{}".format(old_version_partition_path))
        logger.info("Old path info partition:{}".format(old_part_info_for_registering_partition))
    else:
        partition_path_till_frequency = ""
        part_info_for_registering_partition_till_frequency = []
    write_to_target_dir(filtered_df, path_with_partition_info_only, table_path,
                        part_info_for_registering_partition,table_version_path,old_version_partition_path,old_part_info_for_registering_partition)
    check_and_handle_more_versions(partition_path_till_frequency, part_info_for_registering_partition_till_frequency,
                                   new_version, db_name, target_table,
                                   table_path)
    check_and_handle_extra_partitions(new_version, part_info_for_registering_partition_till_frequency,
                                      db_name, target_table)

def audit_entries_2():
    if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'postgre':
        task_completed = 'Y'
        initialize_var_for_postgres_and_insert("SUCCESS", "NA", str(num_rows_in_df), connection)
    if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'hive':
        initialize_var_for_hive_and_insert("SUCCESS", "NA", str(num_rows_in_df))


def check_num_directories(partition_path_till_frequency):
    ls_output = sp.check_output(["hdfs", "dfs", "-ls", partition_path_till_frequency])
    lines = ls_output.decode("utf-8").split("\n")
    num_directories = sum(1 for line in lines if line.startswith("d") and not line.endswith("/.."))
    logger.info("Number of version directories:{}".format(num_directories))
    return num_directories


def get_directories_list(partition_path_till_frequency):
    ls_output = sp.check_output(["hdfs", "dfs", "-ls", partition_path_till_frequency])
    lines = ls_output.decode("utf-8").split("\n")
    directories = [line.split()[-1] for line in lines if line.startswith("d") and not line.endswith("/..")]
    logger.info("List of version directories:{}".format(directories))
    return directories



def check_and_handle_more_versions(partition_path_till_frequency,part_info_for_registering_partition_till_frequency, new_version, db_name, target_table,
                                           table_path):

    partition_path_till_frequency= table_path +"/" + partition_path_till_frequency
    if len(partition_path_till_frequency) == 0:
        logger.info("Versioning is not enabled for current run and skipping the handling of more versions..")
    else:
        logger.info("Partition path till frequency:{}".format(partition_path_till_frequency))
        logger.info("Partition info for registering partition till frequency:{}".format(part_info_for_registering_partition_till_frequency))
        logger.info("Current version:{}".format(new_version))
        logger.info("DB:{}".format(db_name))
        logger.info("Target Table:{}".format(target_table))
        logger.info("Table Path:{}".format(table_path))
        no_of_directories = check_num_directories(partition_path_till_frequency)
        if no_of_directories == 1:
            logger.info("There is single version for the current run and skipping the version checks..")
        else:
            partition_path_till_version = partition_path_till_frequency+"version_val="+new_version
            directories = get_directories_list(partition_path_till_frequency)
            for i in directories:
                if i == partition_path_till_version:
                    logger.info("Path {} is the current version path".format(i))
                else:
                    logger.info("Removing path {} as this is the extra version".format(i))
                    hdfs_path_delete(i)
        logger.info("Extra version checks completed..")

def check_and_handle_extra_partitions(new_version,part_info_for_registering_partition_till_frequency,
                                              db_name, target_table):
    table_name = db_name+"."+target_table
    logger.info("Checking extra partitions for the current job..")
    logger.info("Table Name:{}".format(table_name))
    logger.info("Current version:{}".format(new_version))
    new_part_list = [x for x in part_info_for_registering_partition_till_frequency if "version_val" not in x]
    part_string = ",".join(new_part_list)
    part_stmt_query = "show partitions {0} partition ({1})".format(table_name, part_string)
    logger.info("Show partitions query:{}".format(part_stmt_query))
    df_part = spark.sql(part_stmt_query)
    part_list = [row.partition for row in df_part.collect()]
    logger.info("List of partitions for the current job:{}".format(part_list))
    logger.info("Iterating the list now:")
    for i in part_list:
        if "ods_val" in i and "version_val" in i and new_version in i:
            logger.info("Current partition {} is the latest run and no partition removal is required".format(i))
        elif "ods_val" in i and "version_val" in i and new_version not in i :
            logger.info("Extra partition found:{} and removing it...".format(i))
            alter_string = "ALTER TABLE " + table_name + " DROP IF EXISTS PARTITION ("
            exp_str_list=i.split("/")
            for j in range(len(exp_str_list)):
                if j != len(exp_str_list) - 1:
                    s = exp_str_list[j]
                    initial_string = s[:s.index("=")] + "='" + s[s.index("=") + 1:] + "',"
                    alter_string += initial_string
                else:
                    s = exp_str_list[j]
                    initial_string = s[:s.index("=")] + "='" + s[s.index("=") + 1:] + "')"
                    alter_string += initial_string
            logger.info("Final drop partition command:{}".format(alter_string))
            logger.info("Dropping the current partition {}".format(i))
            spark.sql(alter_string)
        else:
            logger.info("Skipping as current partition {} is corrupted..".format(i))


def check_service_eligibility(service_name):
    if zero_count_check == 'N':
        logger.info("Zero count check is disabled for the current service..")
        return False
    latest_file = max(
        (f for f in os.listdir(mfu_incoming_path) if re.match("allowed_services_\\d{8}\\.csv", f)),
        key=lambda x: dt.datetime.strptime(re.search("\\d{8}", x).group(), "%Y%m%d"),
    ) if any(re.match("allowed_services_\\d{8}\\.csv",f) for f in os.listdir(mfu_incoming_path)) else None
    if latest_file:
        latest_mfu_service_file = os.path.join(mfu_incoming_path,latest_file)
        logger.info("Latest mfu service file :{}".format(latest_mfu_service_file))
        dfp = pd.read_csv(latest_mfu_service_file)
        if service_name in dfp['service_name'].values:
            logger.info("Service {} is in the list of allowed services".format(service_name))
            return False
        else:
            logger.info("Service {} does not exist in the list of allowed services".format(service_name))
            return True
    else:
        logger.info("No matching mfu service file found..")
        return True


def process_data():
    service_file_path = get_service_json_file_path()
    service_json_df = read_service_file_as_df(service_file_path)
    product_name = get_product_name()
    product_active_containers = get_active_containers_for_product(service_json_df, product_name)

    db_name = get_db_name()
    #check_and_create_db_if_not_exists(db_name)

    if is_no_active_containers(product_active_containers):
        active_containers(product_name)
    else:
        audit_entries()
        prod_container_df = get_product_or_service_container_df(service_json_df, product_name)
        service_query = get_service_or_product_sql(prod_container_df)
        target_table = get_target_table_name(prod_container_df)
        '''
        if args.service_name == 'PE_BAL-EBBS-FPSLBS_ALL' and part_col_to_val_or_var_map['country_val'] == 'KE':
            logger.info("Creating ack for PE_BAL-EBBS-FPSLBS_KE and marking job as success")
            create_extra_countries_recon(['KE'])
            with open(filepath + "/" + "success_" + args.service_name.upper() + "_" + dt.datetime.today().strftime(
                    '%Y-%m-%d') + ".txt", "w") as fp:
                logger.info("Touch file creation is successfull")
            sys.exit(0)
  
        if args.service_name == 'ACCT-EBBS-PMMKDEAL_ALL' and part_col_to_val_or_var_map['country_val'] == 'GH':
            logger.info("Creating ack for ACCT-EBBS-PMMKDEAL_GH and marking job as success")
            create_extra_countries_recon(['GH'])
            with open(filepath + "/" + "success_" + args.service_name.upper() + "_" + dt.datetime.today().strftime(
                    '%Y-%m-%d') + ".txt", "w") as fp:
                logger.info("Touch file creation is successfull")
            sys.exit(0)
        if args.service_name == 'ACCT-EBBS-ACCOUNT_ALL' and part_col_to_val_or_var_map['country_val'] == 'GH':
            logger.info("Creating ack for ACCT-EBBS-ACCOUNT_GH and marking job as success")
            create_extra_countries_recon(['GH'])
            with open(filepath + "/" + "success_" + args.service_name.upper() + "_" + dt.datetime.today().strftime(
                    '%Y-%m-%d') + ".txt", "w") as fp:
                logger.info("Touch file creation is successfull")
            sys.exit(0)
        '''
        data_df = spark.sql(service_query)
        data_df.cache()
        global num_rows_in_df,task_completed,new_version
        num_rows_in_df = data_df.count()
        logger.info("Number of rows for the query output : " + str(num_rows_in_df))
        if num_rows_in_df == 0 and check_service_eligibility(args.service_name):
            logger.info("Exiting as the service has no data and not in the allowed list of services..")
            sys.exit(1)
        dynamic_parts_dict = get_dynamic_partition_columns_as_dict_from_part_clause(data_df.columns)
        part_info_for_registering_partition = []
        table_path = get_table_path(prod_container_df)
        table_version_path = get_table_version_path(prod_container_df)
        audit_entries_1()
        if is_dynamic_parts_present(dynamic_parts_dict):
            # logger.info("Number of rows for the query output : " + str(num_rows_in_df))
            distinct_dynamic_partitions_vals_arr = data_df.select(
                list(dynamic_parts_dict.values())).distinct().collect()
            logger.info("Printing  dynamic part values....")
            logger.info(distinct_dynamic_partitions_vals_arr)
            logger.info("Extra countries check started for GPTM/ACBS/SFMP/FPSL/DTP/LTP/COCOA/OTP TP systems..")
            tp_sys(distinct_dynamic_partitions_vals_arr)
            for dynamic_part_col_vals in distinct_dynamic_partitions_vals_arr:
                part_info_for_registering_partition = []
                path_with_partition_info_only = ""
                filtered_df = get_filtered_df(data_df)
                logger.info("Number of rows for the filtered df : " + str(filtered_df.count()))
                is_write_required = True
                partition_col(is_write_required, table_version_path, table_path, dynamic_parts_dict,
                              dynamic_part_col_vals, part_info_for_registering_partition, path_with_partition_info_only,
                              prod_container_df, db_name, filtered_df, target_table)

        else:
            logger.info("All are static partitions")
            filtered_df = get_filtered_df(data_df)
            path_with_partition_info_only = ""
            for part_col_name in part_col_to_val_or_var_map.keys():
                path_with_partition_info_only += part_col_name + "=" + part_col_to_val_or_var_map[part_col_name] + "/"
                logger.info("Path with partition info only:{}".format(path_with_partition_info_only))
                part_info_for_registering_partition.append(
                    part_col_name + equal_zero.format(part_col_to_val_or_var_map[part_col_name]))
                logger.info("part_info_for_registering_partition:{}".format(part_info_for_registering_partition))
            old_version_parition(table_version_path,path_with_partition_info_only,part_info_for_registering_partition,prod_container_df,table_path,filtered_df,db_name,target_table)
    audit_entries_2()

def is_filterkey_equal_to_select_columns(filtered_df):
    return len(set(filter_col_to_val_or_var_map.keys()) & set(filtered_df.columns))


def is_dynamic_val_equal_to_filter(dynamic_part_val):
    isTrue = False
    logging.info("Comparing the " + dynamic_part_val)
    if args.filter_str == "dummy":
        isTrue = True
    else:
        for key, val in filter_col_to_val_or_var_map.items():
            if dynamic_part_val == val:
                isTrue = True
    logging.info("Comparing the " + dynamic_part_val + " is true: " + str(isTrue))
    return isTrue


def get_filtered_df(data_df):
    filtered_df = data_df
    for key, val in filter_col_to_val_or_var_map.items():
        filtered_df = filtered_df.filter("{0}='{1}'".format(key, val))
    return filtered_df


def write_to_target_dir(filtered_df, path_with_partition_info_only, table_path, part_info_for_registering_partition,table_version_path, old_version_partition_path = "", old_part_info_for_registering_partition=[]):
    num_filt_rows_in_df = filtered_df.count()
    country_for_recon = path_with_partition_info_only.split("/")[
        [idx for idx, s in enumerate(path_with_partition_info_only.split("/")) if "country_val" in s][0]].split("=")[1]
    db_name = get_db_name()
    dest_dir_path = table_path + "/" + path_with_partition_info_only
    src_dir_path_version = table_path + "/" + old_version_partition_path
    dest_dir_path_version= table_version_path + "/" + old_version_partition_path
    create_hdfs_dir(dest_dir_path_version)
    logger.info("Writing to target directory path :{}".format(dest_dir_path))
    filtered_df.printSchema()
    target_schema_df = spark.sql("select * from {0}.{1} limit 0".format(db_name, table_name))
    spark.sql("use {0}".format(db_name))
    set_filtered_df = set([name.lower() for name in filtered_df.columns])
    set_target_schema_df = set([name.lower() for name in target_schema_df.columns])
    missinglist = list(sorted(set_target_schema_df - set_filtered_df))
    logger.info("Missing list of columns if any:{}".format(missinglist))
    if len(missinglist) > 0:
        for missing in missinglist:
            if missing in part_col_to_val_or_var_map.keys():
                logger.info(part_col + missing + "  hence skipping the with column")
            elif missing in ["frequency_val", "version_val"]:
                logger.info(part_col + missing + "  hence skipping the with column")
            else:
                filtered_df = filtered_df.withColumn(missing, F.lit(None))
    logger.info("Filtered_df columns:{}".format(filtered_df.columns))
    logger.info("Target_df columns:{}".format(target_schema_df.columns))
    logger.info("Filtered df before transform:{}".format(filtered_df.dtypes))
    filtered_transformed_df = transformed_df1(filtered_df, target_schema_df)
    logger.info("Filtered df after transform:{}".format(filtered_transformed_df.dtypes))
    logger.info("Schema after transform:{}".format(filtered_transformed_df.printSchema()))
    target_schema = spark.sql("select * from {0}.{1} limit 0".format(db_name, table_name)).schema
    logger.info("Schema after converting to target schema :{}".format(filtered_transformed_df.printSchema()))

    # filtered_transformed_df.coalesce(1).write.mode("overwrite").parquet(dest_dir_path)
    logger.info("Checking for current partitions and dropping it")
    # Drop current partition if exists
    check_and_drop_partitions(dest_dir_path, table_name, part_info_for_registering_partition)
    # Write to target directory
    filtered_transformed_df.write.mode("overwrite").parquet(dest_dir_path)
    logger.info("Completed writing to the target path: {}".format(dest_dir_path))
    logger.info("Registering the partition")
    # spark.sql("use {0}".format(db_name))
    '''
    part_stmt = "ALTER TABLE {0} ADD IF NOT EXISTS PARTITION ({1}) LOCATION '{2}'".format(table_name, ",".join(
        part_info_for_registering_partition), dest_dir_path)
    logger.info("Registering the partition stmt :{}".format(part_stmt))
    spark.sql(part_stmt)
    '''
    register_partition(table_name,part_info_for_registering_partition,dest_dir_path)
    if "version_val" in old_version_partition_path:
    # Moving older partition data to version table
        move_data_to_versioned_table(src_dir_path_version, dest_dir_path_version)
    # Registering the partition in version table
        register_partition(table_name + "_version", old_part_info_for_registering_partition, dest_dir_path_version)
    # Check and Drop old partition
        logger.info("Checking for existing partitions and dropping it")
        check_and_drop_partitions(src_dir_path_version, table_name, old_part_info_for_registering_partition)
    write_to_recon(country_for_recon, num_filt_rows_in_df)


# def register_partition(part_clause, partition_location):
def transformed_df1(df, tdf):
    for c, d in tdf.dtypes:
        if c in part_col_to_val_or_var_map.keys():
            logger.info(part_col + c + "  hence skipping the casting")
        elif c in ["frequency_val", "version_val"]:
            logger.info(part_col + c + "  hence skipping the casting")
        else:
            df = df.withColumn(c, col(c).cast(d))
    return df

#Function to move old partition data from product table to versioned table

def move_data_to_versioned_table(src_dir_path_version,dest_dir_path_version):
    logger.info("Moving hdfs data from {} to {}".format(src_dir_path_version,dest_dir_path_version))
    src=src_dir_path_version+"*"
    proc=sp.Popen(['hdfs','dfs','-mv',src,dest_dir_path_version])
    proc.communicate()
    return proc.returncode

#Function to register the partition

def register_partition(table_name,part_info_for_registering_partition,dest_dir_path):
    part_stmt = "ALTER TABLE {0} ADD IF NOT EXISTS PARTITION ({1}) LOCATION '{2}'".format(table_name, ",".join(
        part_info_for_registering_partition), dest_dir_path)
    logger.info("Registering the partition stmt :{}".format(part_stmt))
    spark.sql(part_stmt)



# Function to delete  hdfs folder
def hdfs_path_delete(temppath):
    logger.info('Removing hdfs path:{}'.format(temppath))
    proc = sp.Popen(['hdfs', 'dfs', '-rm', '-r', temppath])
    proc.communicate()
    return proc.returncode


def check_and_drop_partitions(dest_dir_path, table_name, part_info_for_registering_partition):
    dfpart = spark.sql("show partitions " + table_name + " partition (" + ",".join(
        part_info_for_registering_partition) + ")")
    if len(dfpart.select("partition").collect()) == 0:
        logger.info("Selected partition {} is not available and hence drop is not required".format(
            part_info_for_registering_partition))
    else:
        part_stmt = "ALTER TABLE {0} DROP IF EXISTS PARTITION ({1})".format(table_name, ",".join(
            part_info_for_registering_partition))
        spark.sql(part_stmt)
        hdfs_path_delete(dest_dir_path)


def is_dynamic_partition_col(col_name, dynamic_parts_dict):
    return col_name in dynamic_parts_dict


def is_dynamic_parts_present(dynamic_parts_arr):
    return len(dynamic_parts_arr) > 0


def get_dynamic_partition_columns_as_dict_from_part_clause(columns_from_data_frame):
    dynamic_part_dict = {}
    for part_col_name in part_col_to_val_or_var_map.keys():
        part_col_val = part_col_to_val_or_var_map[part_col_name]
        if part_col_val in columns_from_data_frame:
            logger.info("Adding as dynamic partition {} with val {}".format(part_col_name, part_col_val))
            dynamic_part_dict[part_col_name] = part_col_val
    return dynamic_part_dict

def close_cursor_and_open_connection(connection,cur):
    close_cursor(cur)
    logger.info("Creating a new connection as previous connection was closed ")
    connection = create_postgre_connection()
    return connection

def check_interface_error_and_return_connection(connection,cur,e):
    logger.info("Interface error occurred:{}".format(e))
    logger.info(closing_cursor)
    return close_cursor_and_open_connection(connection,cur)

def check_operational_error_and_return_connection(connection,cur,e):
    logger.info("Operational error occurred:{}".format(e))
    logger.info(closing_cursor)
    return close_cursor_and_open_connection(connection,cur)

def check_internal_error_and_return_connection(connection,cur,e):
    logger.info("Internal error occurred:{}".format(e))
    logger.info(closing_cursor)
    return close_cursor_and_open_connection(connection,cur)

def replace_query(table_list,audittablelist,postgre_details,replaced_query):
    if part_col_to_val_or_var_map['tp_sys_val'].upper() == 'XX':
        replaced_query = get_replaced_query_xx(table_list,audittablelist,replaced_query,postgre_details)
    else:
        replaced_query = get_replaced_query(table_list,audittablelist,replaced_query,postgre_details)
    return replaced_query

def audit_table_list(i,audittablelist,postgre_details,replaced_query):
    if (i.lower() in audittablelist or i.upper() in audittablelist) and (
            "${<" + i.split(".")[1] + version.lower() in replaced_query or "${<" + i.split(".")[
        1] + version.upper() in replaced_query):
        logger.info("Table {} is available and version replace to be done".format(i))
        cur = create_cursor(connection)
        max_version = fetch_max_version(cur, postgre_details[7], postgre_details[6], i, connection)
        close_cursor(cur)
        replaced_query = replaced_query.replace("${<" + i.split(".")[1] + version, max_version)
        related_tables[i] = max_version
    elif i.split(".")[0] == args.target_schema and (
            "${<" + i.split(".")[1] + version.lower() in replaced_query or "${<" + i.split(".")[
        1] + version.upper() in replaced_query):
        max_version = fetch_max_version_product(i)
        related_tables[i] = max_version
        if max_version != 'NA':
            replaced_query = replaced_query.replace("${<" + i.split(".")[1] + version, max_version)
    else:
        related_tables[i] = 'NA'
    return replaced_query

def get_service_or_product_sql(prod_container_df):
    query = prod_container_df['SQL'][0]
    logger.info("SQL query for the service is :{}".format(query))
    if args.env == 'prd':
        replaced_query = query.replace("'sub'", "'" + args.ods + "'").replace("'${ods_val}'", "'" + args.ods + "'").replace(
            "${country_val}", part_col_to_val_or_var_map['country_val']).replace("${tp_sys_val}", part_col_to_val_or_var_map['tp_sys_val']).replace(
            "${TP_SYS_VAL}", part_col_to_val_or_var_map['tp_sys_val']).replace("'${frequency_val}'", "'" + args.frequency + "'").replace("'${FREQUENCY_VAL}'", "'" + args.frequency + "'").replace("${env}",  args.env )
    else:
        replaced_query = query.replace("'sub'", "'" + args.ods + "'").replace("'${ods_val}'",
                                                                              "'" + args.ods + "'").replace(
            "${country_val}", part_col_to_val_or_var_map['country_val']).replace("${tp_sys_val}",
                                                                                 part_col_to_val_or_var_map[
                                                                                     'tp_sys_val']).replace(
            "${TP_SYS_VAL}", part_col_to_val_or_var_map['tp_sys_val']).replace("'${frequency_val}'",
                                                                               "'" + args.frequency + "'").replace(
            "'${FREQUENCY_VAL}'", "'" + args.frequency + "'")
        pattern = re.compile(r'fdp_daas_\${env}',re.IGNORECASE)
        replaced_query = pattern.sub('fdp_daas_' +args.env, replaced_query)
        replaced_query = replaced_query.replace("${env}",  "dev" )

    logger.info("Replaced SQL query for the service after partiton var replace is :{}".format(replaced_query))
    # Commenting below as this is not a full fledged versioning rollout
    if version_enabled == 'Y':
        table_list = get_table_names(replaced_query)
        key_list = get_keys_for_postgre()
        environment = args.env
        postgre_details = db_connect.get_postgre_details(environment, key_list[0], key_list[1], key_list[2],
                                                         key_list[3],
                                                         key_list[4], key_list[5], key_list[6], key_list[7])
        cur = create_cursor(connection)
        audittablelist = fetch_target_table_list(cur, postgre_details[7], postgre_details[6], connection)
        close_cursor(cur)
        logger.info("Audittablelist:{}".format(audittablelist))
        logger.info("Tablelist:{}".format(table_list))
        logger.info("Checking for in clause of version tables for query update")
        replaced_query = replace_query(table_list,audittablelist,postgre_details,replaced_query)
        for i in table_list:
            replaced_query = audit_table_list(i,audittablelist,postgre_details,replaced_query)
    logger.info("Final Replaced SQL query for the service is :{}".format(replaced_query))
    return replaced_query

def get_replaced_query(table_list,audittablelist,replaced_query,postgre_details):
    for i in table_list:
        if (i.lower() in audittablelist or i.upper() in audittablelist) and (
                in_param + i.split(".")[1] + lower_version.lower() in replaced_query or in_param + i.split(".")[
            1] + lower_version.upper() in replaced_query or "IN"+less_than + i.split(".")[1] + lower_version.lower() in replaced_query
                or "in" + less_than + i.split(".")[
                    1] + lower_version.upper() in replaced_query
        ):
            logger.info("Table {} with version in clause is available and version replace to be done".format(i))
            cur = create_cursor(connection)
            max_version = fetch_max_version_tp(cur, postgre_details[7], postgre_details[6], i, connection)
            maxversion = fetch_max_version(cur, postgre_details[7], postgre_details[6], i, connection)
            logger.info("Max version fetched for table {} is {}".format(i,max_version))
            close_cursor(cur)
            replaced_query = replaced_query.replace(in_param + i.split(".")[1] + lower_version.lower(),
                                                    in_condition + max_version + "')").replace(in_param + i.split(".")[
                1] + lower_version.upper(), IN_CONDITION + max_version + "')").replace(
                "IN" + less_than + i.split(".")[1] + lower_version.lower(), IN_CONDITION + max_version + "')").replace("in" + less_than + i.split(".")[
                1] + lower_version.upper(), in_condition + max_version + "')")
            related_tables[i] = maxversion
        elif i.split(".")[0] == args.target_schema and (in_param + i.split(".")[1] + lower_version.lower() in replaced_query or in_param + i.split(".")[
            1] + lower_version.upper() in replaced_query or "IN"+less_than + i.split(".")[1] + lower_version.lower() in replaced_query
                or "in" + less_than + i.split(".")[
                    1] + lower_version.upper() in replaced_query):
            logger.info("Product Table {} with version in clause is available and version replace to be done".format(i))
            max_version = fetch_max_version_product_tp(i)
            maxversion = fetch_max_version_product(i)
            logger.info("Max version fetched for table {} is {}".format(i, max_version))
            related_tables[i] = maxversion
            if max_version != 'NA':
                replaced_query = replaced_query.replace(in_param + i.split(".")[1] + lower_version.lower(),
                                                    in_condition + max_version + "')").replace(in_param + i.split(".")[
                1] + lower_version.upper(), IN_CONDITION + max_version + "')").replace(
                "IN" + less_than + i.split(".")[1] + lower_version.lower(), IN_CONDITION + max_version + "')").replace("in" + less_than + i.split(".")[
                1] + lower_version.upper(), in_condition + max_version + "')")
        else:
            logger.info("{} table does not have in clause for versioning".format(i))
    return replaced_query


def get_replaced_query_xx(table_list,audittablelist,replaced_query,postgre_details):
    for i in table_list:
        if (i.lower() in audittablelist or i.upper() in audittablelist) and (
                in_param + i.split(".")[1] + lower_version.lower() in replaced_query or in_param + i.split(".")[
            1] + lower_version.upper() in replaced_query or "IN" + less_than + i.split(".")[
                    1] + lower_version.lower() in replaced_query
                or "in" + less_than + i.split(".")[
                    1] + lower_version.upper() in replaced_query
        ):
            logger.info("Table {} and TP sys XX with version in clause is available and version replace to be done".format(i))
            cur = create_cursor(connection)
            version_string = fetch_multiple_versions_tp_xx(cur,postgre_details[7],postgre_details[6],i,connection)
            logger.info("List of versions:{}".format(version_string))
            max_version = fetch_max_version(cur, postgre_details[7], postgre_details[6], i, connection)
            replaced_query = replaced_query.replace(in_param + i.split(".")[1] + lower_version.lower(),
                                                    "in (" + version_string + ")").replace(in_param + i.split(".")[
                1] + lower_version.upper(), "IN (" + version_string + ")").replace(
                "IN" + less_than + i.split(".")[1] + lower_version.lower(), "IN (" + version_string + ")").replace(
                "in" + less_than + i.split(".")[
                    1] + lower_version.upper(), "in (" + version_string + ")")
            related_tables[i] = max_version
        elif i.split(".")[0] == args.target_schema and (
                in_param + i.split(".")[1] + lower_version.lower() in replaced_query or in_param + i.split(".")[
            1] + lower_version.upper() in replaced_query or "IN" + less_than + i.split(".")[
                    1] + lower_version.lower() in replaced_query
                or "in" + less_than + i.split(".")[
                    1] + lower_version.upper() in replaced_query):
            logger.info("Product Table {} and TP sys XX with version in clause is available and version replace to be done".format(i))
            max_version = fetch_max_version_product(i)
            version_string = fetch_multiple_versions_product_tp_xx(i)
            logger.info("List of versions:{}".format(version_string))
            replaced_query = replaced_query.replace(in_param + i.split(".")[1] + lower_version.lower(),
                                                    "in (" + version_string + ")").replace(in_param + i.split(".")[
                1] + lower_version.upper(), "IN (" + version_string + ")").replace(
                "IN" + less_than + i.split(".")[1] + lower_version.lower(), "IN (" + version_string + ")").replace(
                "in" + less_than + i.split(".")[
                    1] + lower_version.upper(), "in (" + version_string + ")")
            related_tables[i] = max_version
        else:
            logger.info("{} table with TP sys XX does not have in clause for versioning".format(i))
    return replaced_query



def fetch_multiple_versions_tp_xx(cur,schema,table,i,connection):
    try:
        logger.info("Below command to execute:")
        logger.info("select max(version) as max_version,tp from " + schema + "." + table + where_ods + args.ods + "'" + lower_target_table + i.lower() + "'" + freq + args.frequency + "'"
            + " group by tp")
        cur.execute("select max(version) as max_version,tp from " + schema + "." + table + where_ods + args.ods + "'" + lower_target_table + i.lower() + "'" + freq + args.frequency + "'"
            + " group by tp")
        tp_and_version_list = cur.fetchall()
        logger.info("TP and max version details as follows:{}".format(tp_and_version_list))
        version_list = [x[0] for x in tp_and_version_list]
        version_list=list(set(version_list))
        version_string = get_version_string_from_list(version_list)
        return version_string
    except InterfaceError as e:
        connection = check_interface_error_and_return_connection(connection, cur, e)
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        logger.info("Calling fetch_multiple_versions_tp_xx function")
        return fetch_multiple_versions_tp_xx(cur, schema, table, i, connection)
    except OperationalError as e:
        connection = check_operational_error_and_return_connection(connection, cur, e)
        logger.info(creating_cursor)
        cur = create_cursor(connection)
        logger.info("Calling fetch_multiple_versions_tp_xx function")
        return fetch_multiple_versions_tp_xx(cur, schema, table, i, connection)
    except InternalError as e:
        cursor_con=create_cursor(connection)
        connection = check_operational_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(internal_job_run_status, internal_exception_details, internal_rowcount,
                                               connection)


def fetch_multiple_versions_product_tp_xx(table):
    where_clause = ods_val + args.ods + freq_val + args.frequency + "'"
    logger.info(prodver_query)
    logger.info("select max(version_val) as version_val,tp_sys_val from " + table + where + where_clause + "group by tp_sys_val")
    dfv=spark.sql("select max(version_val) as version_val,tp_sys_val from " + table + where + where_clause + "group by tp_sys_val")
    tp_and_version_list = [(row.version_val, row.tp_sys_val) for row in dfv.collect()]
    logger.info("TP and max version details as follows:{}".format(tp_and_version_list))
    version_list = [x[0] for x in tp_and_version_list]
    version_list = list(set(version_list))
    version_string = get_version_string_from_list(version_list)
    return version_string

def get_version_string_from_list(version_list):
    version_string=""
    for i in range(len(version_list)):
        if i!=len(version_list) - 1:
            version_string = version_string + "'" + version_list[i] + "'" + ","
        else:
            version_string = version_string + "'" + version_list[i] + "'"
    return version_string



def check_and_create_db_if_not_exists(db_name):
    db_existence_df = spark.sql("show databases like '" + db_name + "'")
    if db_existence_df.count == 0:
        db_path = get_config_value("hdfs_path") + db_name.lower()
        logger.info("Database {0} does not exists. Going to create it with db_path {1}".format(db_name, db_path))
        db_creation_command = "create database " + db_name + " location " + "'" + db_path + "'"
        logger.info("DB creation command {}".format(db_creation_command))
        spark.sql(db_creation_command)
    else:
        logger.info("Target database {0} already exists. So, skipping the creation part".format(db_name))


def get_table_path(prod_container_df):
    db_name = get_db_name()
    global table_name
    table_name = get_target_table_name(prod_container_df)
    return get_table_path_from_db_tb(db_name, table_name)


def get_table_version_path(prod_container_df):
    db_name = get_db_name()
    global table_name_version
    table_name_version = get_target_table_name(prod_container_df) +"_version"
    return get_table_path_from_db_tb(db_name, table_name_version)



def get_table_path_from_db_tb(db_name, table_name):
    table_path = get_config_value("hdfs_path") + db_name.lower() + "/" + table_name.lower()
    logger.info("Table path for {0} db and {1} table is {2}".format(db_name, table_name, table_path))
    return table_path


def get_target_table_name(prod_container_df):
    return prod_container_df['Target_Table'][0]


def get_db_name():
    return args.target_schema


def is_no_active_containers(product_active_containers):
    return len(product_active_containers) == 0


def get_product_or_service_container_df(service_json_df, product_name):
    success_status_df = service_json_df[service_json_df['Status'] == 'Success']
    active_success_status_df = success_status_df[success_status_df['Active'] == 'A']
    return active_success_status_df[active_success_status_df['Service Request'] == product_name].reset_index()


def get_active_containers_for_product(service_json_df, product_name):
    success_status_df = service_json_df[service_json_df['Status'] == 'Success']
    active_services_df = list(success_status_df[success_status_df['Active'] == 'A']['Service Request'])
    product_containers = [service for service in active_services_df if service.upper() == product_name.upper()]
    return product_containers


def get_part_col_to_val_var_map():
    return part_col_to_val_or_var_map


global filter_col_to_val_or_var_map
filter_col_to_val_or_var_map = {}


def build_filter_map(filter_str):
    if (filter_str != "dummy"):
        filter_vars_arr = filter_str.split(";")
        for filter_var in filter_vars_arr:
            filter_to_val_or_var = filter_var.split("=")
            filter_col_to_val_or_var_map[filter_to_val_or_var[0]] = filter_to_val_or_var[1]
    logger.info("Getting the partition col to val map {}".format(str(filter_col_to_val_or_var_map)))


def build_partition_map(part_vars_str):
    part_vars_arr = part_vars_str.split(";")
    for part_var in part_vars_arr:
        part_to_val_or_var = part_var.split("=")
        part_col_to_val_or_var_map[part_to_val_or_var[0]] = part_to_val_or_var[1]
    logger.info("Getting the partition col to val map {}".format(str(part_col_to_val_or_var_map)))


def get_product_name():
    return args.product_name


def get_service_json_file_path():
    return args.service_file_path


def read_service_file_as_df(json_file_name):
    logger.info("Reading the json file {} ".format(json_file_name))
    return pd.read_json(json_file_name)


def build_config_map(env, env_file_path):
    logger.info("Reading the env {0} from {1} location ".format(env, env_file_path))
    config_parser = configparser.ConfigParser()
    config_parser.read(env_file_path)
    config_map = dict(config_parser.items(env))
    return config_map


def get_config_value(key):
    return config_map[key]


def get_keys_for_postgre():
    tablekey = 'postgre_table'
    schemakey = 'postgre_schema'
    userkey = 'postgre_user'
    hostkey = 'postgre_host'
    portkey = 'postgre_port'
    dbkey = 'postgre_database'
    stgtablekey = 'postgre_stg_table'
    stgschemakey = 'postgre_stg_schema'
    daasversiontablekey = 'postgre_daas_version_table'
    keylist = [tablekey, schemakey, userkey, hostkey, portkey, dbkey, stgtablekey, stgschemakey, daasversiontablekey]
    return keylist

def create_postgre_daas_audit_table():
    key_list = get_keys_for_postgre()
    environment = args.env
    postgre_details = db_connect.get_postgre_details(environment, key_list[0], key_list[1], key_list[2],
                                                     key_list[3],
                                                     key_list[4], key_list[5], key_list[6], key_list[7], key_list[8])
    schema = postgre_details[1]
    table = postgre_details[0]
    sql = """create table if not exists """ + schema + """.""" + table + """(country text,
        tpsystem text,
        ods DATE,
        product_name text,
        service_name text,
        rowcount text,
		log_file_name text,
        target_table text,
        start_time TIMESTAMP,
        job_run_time text,
        job_run_status text,
        exception_details text,
        json_file_name text,
        processid_start_time text
        )"""
    db_connect.create_table_in_postgre(connection, logger, sql)



def create_postgre_daas_version_table():
    key_list = get_keys_for_postgre()
    environment = args.env
    postgre_details = db_connect.get_postgre_details(environment, key_list[0], key_list[1], key_list[2],
                                                     key_list[3],
                                                     key_list[4], key_list[5], key_list[6], key_list[7], key_list[8])
    schema = postgre_details[1]
    table = postgre_details[8]
    sql = """create table if not exists """ + schema + """.""" + table + """(country text,
        tpsystem text,
        ods DATE,
        product_name text,
        service_name text,
        rowcount text,
        log_file_name text,
        target_table text,
        start_time TIMESTAMP,
        job_run_time text,
        job_run_status text,
        exception_details text,
        json_file_name text,
        processid_start_time text,
        product_version text,
        related_tables text,
        frequency text,
        job_end_time TIMESTAMP
        )"""
    db_connect.create_table_in_postgre(connection, logger, sql)


def write_to_recon(country_for_recon, num_filt_rows_in_df):
    logger.info("Writing for recon")
    product_name = args.product_name_upper
    country = country_for_recon
    tp = args.partition_vars.split(";")[2].split("=")[1]
    processid = args.partition_vars.split(";")[3].split("=")[1]
    ods = args.ods
    ods_country_tp_processid = ods + "_" + country + "_" + tp + "_" + processid
    local_path = filepath + "/" + product_name + "_" + country + "_" + tp + "_" + processid + ".txt"
    filewrite = open(local_path, "w")
    filewrite.write("ods_country_tp_processid|count" + "\n")
    filewrite.write(ods_country_tp_processid + "|" + str(num_filt_rows_in_df) + "\n")
    filewrite.close()
    # Create hdfs folder
    cmd = "hdfs dfs -mkdir -p " + hdfspath + "recon/" + ods + "/" + product_name.lower()
    logger.info("Creating hdfs folder:{}".format(cmd))
    proc = sp.Popen(cmd, shell = True, stdout = sp.PIPE, stderr = sp.PIPE)
    proc.communicate()
    cmd3 = "hdfs dfs -mkdir -p " + hdfs_unencrypted_path + "recon/" + ods + "/" + product_name.lower()
    logger.info("Creating encrypted hdfs folder:{}".format(cmd3))
    proc = sp.Popen(cmd3, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    proc.communicate()
    # Move file to hdfs
    cmd1 = "hdfs dfs -put -f " + local_path + " " + hdfspath + "recon/" + ods + "/" + product_name.lower()
    logger.info("Moving file to hdfs:{}".format(cmd1))
    proc = sp.Popen(cmd1, shell = True, stdout = sp.PIPE, stderr = sp.PIPE)
    proc.communicate()

    cmd4 = "hdfs dfs -put -f " + local_path + " " + hdfs_unencrypted_path + "recon/" + ods + "/" + product_name.lower()
    logger.info("Moving file to hdfs:{}".format(cmd4))
    proc = sp.Popen(cmd4, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    proc.communicate()

    # Delete file from NAS
    cmd2 = "rm " + local_path
    logger.info("Removing file from NAS:{}".format(cmd2))
    proc = sp.Popen(cmd2, shell = True, stdout = sp.PIPE, stderr = sp.PIPE)
    proc.communicate()


def initialize_var_for_hive_and_insert(job_run_status, exception_details, rowcount):
    logger.info("Initializing vars for hive audit insert")
    time_end = dt.datetime.now()
    country = part_col_to_val_or_var_map['country_val']
    ods = part_col_to_val_or_var_map['ods_val']
    tpsystem = part_col_to_val_or_var_map['tp_sys_val']
    processid = part_col_to_val_or_var_map['process_id_val']
    product_name = args.product_name_upper
    service_name = args.service_name
    log_file_name = logger_path + args.product_name + "_DP_" + appName + "_" + current_datetime + ".log"
    target_table = args.product_name_upper
    environment = args.env
    start_time = str(time_start)
    time_elapsed_min = ((time_end - time_start).total_seconds()) / 60
    job_run_time = str(time_elapsed_min)
    hive_table_name = hive_audit_table
    hive_schema_name = hive_audit_schema
    logger.info("Hive table and schema details:{},{}".format(hive_table_name, hive_schema_name))
    if exception_details != 'NA':
        exception_details = "Exception occured..Please check {} for exception stack trace".format(log_file_name)
    json_file_name = args.service_file_path
    processid_start_time = processid + start_time
    record = [country, tpsystem, ods, product_name, service_name, rowcount, log_file_name,
              target_table, start_time, job_run_time, job_run_status, exception_details, json_file_name,
              processid_start_time]
    logger.info("Inserting data to hive audit table")
    sql_insert_query = " INSERT INTO " + hive_table_name + "." + hive_schema_name + " " + "(country,tpsystem,ods," \
                                                                                          "product_name,service_name," \
                                                                                          "rowcount,log_file_name," \
                                                                                          "target_table,start_time," \
                                                                                          "job_run_time," \
                                                                                          "job_run_status," \
                                                                                          "exception_details," \
                                                                                          "json_file_name," \
                                                                                          "processid_start_time) " \
                                                                                          "VALUES ({})".format(
        ",".join("'" + x + "'" for x in record))

    spark.sql(sql_insert_query)

def initialize_var_for_postgres_and_insert(job_run_status, exception_details, rowcount, connection):
    try:
        logger.info("Initializing vars for postgres audit insert")
        time_end = dt.datetime.now()
        country = part_col_to_val_or_var_map['country_val']
        ods = part_col_to_val_or_var_map['ods_val']
        tpsystem = part_col_to_val_or_var_map['tp_sys_val']
        processid = part_col_to_val_or_var_map['process_id_val']
        product_name = args.product_name_upper
        service_name = args.service_name
        log_file_name = logger_path + args.product_name + "_DP_" + appName + "_" + current_datetime + ".log"
        target_table = args.product_name_upper
        environment = args.env
        start_time = str(time_start)
        time_elapsed_min = ((time_end - time_start).total_seconds()) / 60
        job_run_time = str(time_elapsed_min)
        job_end_time = str(time_end)
        key_list = get_keys_for_postgre()
        postgre_details = db_connect.get_postgre_details(environment, key_list[0], key_list[1], key_list[2], key_list[3],
                                                         key_list[4], key_list[5], key_list[6], key_list[7], key_list[8])
        postgre_table_name = postgre_details[0]
        postgre_schema_name = postgre_details[1]
        postgre_daas_version_table = postgre_details[8]
        rt = str(related_tables)
        logger.info("Postgre table and schema details:{},{}".format(postgre_table_name, postgre_schema_name))
        check_if_record_exist = "SELECT count(*) FROM " + postgre_schema_name + "." + postgre_table_name + " where country= '" + country + "' and tpsystem='" + tpsystem + "' and ods='" + ods + "' and product_name='" + product_name + "'and service_name='" + service_name + "' and target_table='" + target_table + "' and start_time='" + start_time + "'"
        cursor_con = create_cursor(connection)
        cursor_con.execute(check_if_record_exist)
        result = cursor_con.fetchone()
        cursor_con.close()
        if result[0] == 0:
            logger.info(
                "Enters If condition as check_if_record_exist has no rows i,e new insertion of product to audit table")
            sql_insert_query = " INSERT INTO " + postgre_schema_name + "." + postgre_table_name + " " + "(country,tpsystem,ods," \
                                                                                                        "product_name,service_name," \
                                                                                                        "rowcount,log_file_name," \
                                                                                                        "target_table,start_time," \
                                                                                                        "job_run_time," \
                                                                                                        "job_run_status," \
                                                                                                        "exception_details," \
                                                                                                        "json_file_name," \
                                                                                                        "processid_start_time) " \
                                                                                                        "VALUES (%s,%s,%s,%s,%s," \
                                                                                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        else:
            logger.info(
                "Enters Else condition as check_if_record_exist has some rows hence update the same row with next level job run status in postgres Audit Table")
            sql_update_query = " UPDATE " + postgre_schema_name + "." + postgre_table_name + " SET job_run_status ='" + job_run_status + "',rowcount='" + rowcount + "', job_run_time='" + job_run_time + "'  WHERE country= '" + country + "' and tpsystem='" + tpsystem + "' and ods='" + ods + "' and product_name='" + product_name + "'and service_name='" + service_name + "' and target_table='" + target_table + "' and start_time='" + start_time + "'"
        if exception_details != 'NA':
            exception_details = "Exception occured..Please check {} for exception stack trace".format(log_file_name)
        json_file_name = args.service_file_path
        processid_start_time = processid + start_time
        record = [(country, tpsystem, ods, product_name, service_name, rowcount, log_file_name,
                   target_table, start_time, job_run_time, job_run_status, exception_details, json_file_name,
                   processid_start_time)]
        logger.info("Calling postgres data insert function")
        if result[0]==0:
            db_connect.data_insert_to_postgress(record, connection, logger, sql_insert_query)
            logger.info(
                "Task Completed:{} , Job Run Status:{} ,New_Version:{}".format(task_completed, job_run_status, new_version))
        else:
            db_connect.data_update_to_postgress(connection, logger, sql_update_query)
        if job_run_status == 'SUCCESS':
            logger.info("The Job status is SUCCESS. Hence, creating the ACK file.")
            if task_completed == 'Y' and version_enabled == 'Y':
                product_version = new_version
                sql_insert_query_next = " INSERT INTO " + postgre_schema_name + "." + postgre_daas_version_table + " " + "(country,tpsystem,ods," \
                                                                                                                         "product_name,service_name," \
                                                                                                                         "rowcount,log_file_name," \
                                                                                                                         "target_table,start_time," \
                                                                                                                         "job_run_time," \
                                                                                                                         "job_run_status," \
                                                                                                                         "exception_details," \
                                                                                                                         "json_file_name," \
                                                                                                                         "processid_start_time,product_version," \
                                                                                                                         "related_tables,frequency,job_end_time) " \
                                                                                                                         "VALUES (%s,%s,%s,%s,%s," \
                                                                                                                         "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "

                record_next = [(country, tpsystem, ods, product_name, service_name, rowcount, log_file_name,
                                target_table, start_time, job_run_time, job_run_status, exception_details, json_file_name,
                                processid_start_time, product_version, rt, args.frequency, job_end_time)]

                logger.info("Calling postgres data insert function for product version table")
                db_connect.data_insert_to_postgress(record_next, connection, logger, sql_insert_query_next)
                create_ack_file(country, tpsystem, ods, product_name, service_name, rowcount, target_table, start_time,
                                job_run_status, product_version)
    except InterfaceError as e:
        cursor_con=create_cursor(connection)
        connection = check_interface_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(job_run_status, exception_details, rowcount, connection)
    except OperationalError as e:
        cursor_con=create_cursor(connection)
        connection = check_operational_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(job_run_status, exception_details, rowcount, connection)
    except InternalError as e:
        cursor_con=create_cursor(connection)
        connection = check_operational_error_and_return_connection(connection, cursor_con, e)
        logger.info(calling_var_postgres)
        initialize_var_for_postgres_and_insert(job_run_status, exception_details, rowcount, connection)


# This method is called to create recon file after successful batch run
def create_ack_file(country, tpsystem, ods, product_name, service_name, rowcount, target_table, start_time,
                    job_run_status, product_version):
    if args.frequency.lower() == "daily":
        file_frequency = "D"
    elif args.frequency.lower() == "monthly":
        file_frequency = "M"
    elif args.frequency.lower() == "quarterly":
        file_frequency = "Q"
    elif args.frequency.lower() == "yearly":
        file_frequency = "Y"

    recon_file = nas_base_path + "/" + args.env + "/data/control_file/" + product_name + "_" + file_frequency + "_" + ods + "_" + product_version.split("_")[0] + ".txt"
    with open(recon_file, "w", newline = '') as file:
        writer = csv.writer(file, delimiter = '\x01')
        writer.writerow(
            ["country", "tpsystem", "ods", "product_name", "service_name", "rowcount", "target_table", "start_time",
             "job_run_status", "product_version"])
        writer.writerow(
            [country, tpsystem, ods, product_name, service_name, rowcount, target_table, start_time, job_run_status,
             product_version])


def create_postgre_connection():
    logger.info("Getting keys for postgre connectivity")
    key_list = get_keys_for_postgre()
    logger.info("Connecting to postgres db")
    connection = db_connect.connect(args.env, key_list[0], key_list[1], key_list[2], key_list[3], key_list[4],
                                    key_list[5], key_list[6], key_list[7], key_list[8])
    return connection


# This method is called only to facilitate test cases run
def receive_args_and_call_function(f):
    global args
    args = argparse.Namespace(country = 'ALL', env = 'dev',
                              env_file_path = '/CTRLFW/FDP/dev/daas/pyspark_scripts/config.env', ods = '2022-06-30',
                              partition_vars = 'ods_val=2022-06-30;country_val=CTRY_CD;tp_sys_val=FPSL;process_id_val=SUB_LDGR-FPSL-GPTM-ACTG',
                              filter_str = 'CTRY_CD=SG',
                              product_name = 'SUB_LDGR-FPSL-GPTM-ACTG', product_name_upper = 'SUB_LDGR',
                              service_file_path = '/CTRLFW/FDP/dev/daas/sql//DP_SUB_LDGR_20230304-111457AM.json',
                              service_name = 'SUB_LDGR-FPSL-GPTM-ACTG', target_schema = 'fdp_daas_dev', tp_sys = None, frequency='daily')
    global config_map,beeline_string,logger_path,appName,current_datetime,time_start,hive_audit_schema,hive_audit_table,connection
    config = configparser.ConfigParser()
    config.read(args.env_file_path)
    logger_path = config[args.env]['logger_path']
    beeline_string = config[args.env]['beeline_string']
    hive_audit_table = config[args.env]['audittable']
    hive_audit_schema = config[args.env]['reports_schema']
    appName = config[args.env]['appName']
    time_start = dt.datetime.now()
    current_datetime = str(dt.datetime.now().strftime(date_format))
    logging_file_name = logger_path + args.product_name + "_DP_FDPAPP" + "_" + current_datetime + ".log"
    current_datetime = str(dt.datetime.now().strftime(date_format))
    key_list=get_keys_for_postgre()
    connection = db_connect.connect(args.env, key_list[0], key_list[1], key_list[2], key_list[3], key_list[4],
                                    key_list[5], key_list[6], key_list[7], key_list[8])
    global logger
    logger = getlogger.get_logger(logging_file_name)
    config_map = build_config_map(args.env, args.env_file_path)
    global part_col_to_val_or_var_map
    part_col_to_val_or_var_map = OrderedDict()
    build_partition_map(args.partition_vars)
    global filter_col_to_val_or_var_map
    filter_col_to_val_or_var_map = {}
    build_filter_map(args.filter_str)
    global db_name
    global table_name
    db_name = "fdp_daas_dev"
    table_name = "sub_ldgr"
    return eval(f)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type = str, required = True)
    parser.add_argument('--env_file_path', type = str, required = True)
    parser.add_argument('--product_name', type = str, required = True)
    parser.add_argument('--country', type = str, required = False)
    parser.add_argument('--ods', type = str, required = True)
    parser.add_argument('--tp_sys', type = str, required = False)
    parser.add_argument('--partition_vars', type = str, required = False)
    parser.add_argument('--filter_str', type = str, required = False)
    parser.add_argument('--service_file_path', type = str, required = False)
    parser.add_argument('--target_schema', type = str, required = False)
    parser.add_argument('--service_name', type = str, required = False)
    parser.add_argument('--product_name_upper', type = str, required = True)
    parser.add_argument('--frequency', type = str, required = True)
    parser.add_argument('--country_val',type = str, required = True)
    parser.add_argument('--tp_sys_val', type=str, required=True)
    # parser.add_argument('--help', type=str, required=False)
    return parser


if __name__ == "__main__":
    try:
        # logger.info("Parsing the args")
        parser = parse_args()
        global args
        args = parser.parse_args()
        global time_start, new_version, task_completed, related_tables,country_str,mfu_incoming_path,zero_count_check
        task_completed = 'N'
        new_version = ''
        related_tables = {}
        time_start = dt.datetime.now()
        nas_base_path = db_connect.get_base_path(args.env)
        config = configparser.ConfigParser()
        configforpostgre = configparser.ConfigParser()
        config.read(nas_base_path + args.env + '/daas/pyspark_scripts/config_version.env')
        log_path_append = str(args.ods) + "/" + str(args.product_name_upper).lower() + "/"
        logger_path = config[args.env]['logger_path'] + log_path_append
        appName = config[args.env]['appName']
        audit_storage_config_loc = config[args.env]['audit_storage_config_loc']
        print(audit_storage_config_loc)
        configforpostgre.read(audit_storage_config_loc)
        audit_entries_storage_mode = configforpostgre[args.env]['audit_entries_storage_mode']
        mfu_incoming_path = config[args.env]['mfu_incoming_path']
        zero_count_check = config[args.env]['zero_count_check']
        if audit_entries_storage_mode == 'hive':
            hive_audit_table = config[args.env]['hive_audit_table_name']
            hive_audit_schema = config[args.env]['hive_audit_schema_name']
        is_audit_entries_enabled = configforpostgre[args.env]['is_audit_entries_enabled']
        global beeline_string, logging_file_name, logger, config_map, part_col_to_val_or_var_map, connection, filter_map, version_enabled
        beeline_string = config[args.env]['beeline_string']
        spark = get_spark_session(appName)
        current_datetime = str(dt.datetime.now().strftime("%Y-%m-%d-%H%M%S"))
        part_col_to_val_or_var_map = OrderedDict()
        logging_file_name = logger_path + args.product_name + "_" + args.country_val + "_" + \
                            args.tp_sys_val + "_DP_" + appName + "_" + current_datetime + ".log"
        logger = getlogger.get_logger(logging_file_name)
        config_map = build_config_map(args.env, args.env_file_path)
        build_partition_map(args.partition_vars)
        build_filter_map(args.filter_str)
        logger.info("Product Name :{},ODS:{}".format(args.product_name, args.ods))
        logger.info("Partition map details:{}".format(str(part_col_to_val_or_var_map)))
        if is_audit_entries_enabled == 'Y':
            connection=create_postgre_connection()
        logger.info("DDL checks in progress....")
        # process_ddl()
        logger.info("DDL checks is completed..")
        logger.info("Starting with processing..")
        filepath = config[args.env]['base_path']
        hdfspath = config[args.env]['hdfs_path']
        hdfs_unencrypted_path = hdfspath.replace("_enc/","/")
        version_enabled = config[args.env]['version_enabled']
        create_postgre_daas_audit_table()
        create_postgre_daas_version_table()
        if (part_col_to_val_or_var_map['tp_sys_val'] == 'GPTM') or (part_col_to_val_or_var_map['tp_sys_val'] == 'DTP') or (part_col_to_val_or_var_map['tp_sys_val'] == 'FPSL') or (part_col_to_val_or_var_map['tp_sys_val'] == 'LTP') or (part_col_to_val_or_var_map['tp_sys_val'] == 'COCOA') or (part_col_to_val_or_var_map['tp_sys_val'] == 'OTP'):
            country_str = ["GB", "JP", "SA", "GM", "ZA", "BD", "CN", "HK", "KE", "ZM", "IQ", "ZW", "PH", "KR", "JE",
                           "MO","AE", "JO", "VN", "NP", "DE", "SG", "MU", "AO", "NG", "TW", "BN", "US", "MY", "OM", "CI",
                           "SL","TH", "AU", "EG", "BH", "PK", "CM", "TZ", "LK", "DF", "BW", "QA", "GH", "UG", "ID", "IN", "LN", "SP"]
        elif part_col_to_val_or_var_map['tp_sys_val'] == 'SFMP':
            country_str = ["EG", "NP", "SA", "CN", "GB"]
        elif part_col_to_val_or_var_map['tp_sys_val'] == 'ACBS':
            country_str=["ZW","ZA","TZ","SL","NG","ZM","UG","KE","GH","GM","CI","CM","BW","AO","US","JP","AU","MU","PK","SH",
                         "IN","VN","MY","TW","SG","TH","PH","MO","HK","LN","KH","CN","GB","DE","QA","SA","OM","EG","IQ","AE",
                         "BD","BO","BH","DI"]
        elif part_col_to_val_or_var_map['tp_sys_val'] == 'S2BL':
            country_str = ["BW", "CI", "GH", "ID", "JP", "KE", "LK", "MU", "NG", "PH", "TH", "TW", "UG", "US", "VN",
                           "ZA", "ZM", "AE", "AO", "BH", "CM", "CN", "DF", "EG", "HK", "IN", "MY", "OM", "PK", "QA",
                           "SG", "TZ", "SA"]
        else:
            logger.info("Country List is not required for the subject TP")
        process_data()
        logger.info("Processing is completed..")
        with open(filepath + "/" + "success_" + args.service_name.upper() + "_" + part_col_to_val_or_var_map['country_val'] + "_" + part_col_to_val_or_var_map['tp_sys_val'] + "_" + dt.datetime.today().strftime(
                '%Y-%m-%d') + ".txt", "w") as fp:
            logger.info("Touch file creation is successful")
    except Exception as e:
        if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'postgre':
            initialize_var_for_postgres_and_insert("FAILED", "Exception occured", "NA", connection)
        if is_audit_entries_enabled == 'Y' and audit_entries_storage_mode == 'hive':
            initialize_var_for_hive_and_insert("FAILED", "Exception occured", "NA")
        logger.error("Exception occurred", exc_info = True)
        logger.info("*******************************************************")
