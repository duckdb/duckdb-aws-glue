#!/usr/bin/env bash
# Deletes the Glue tables (in every database of the account) whose Iceberg metadata_location starts with the given S3
# prefix, through Glue's DeleteTable: a table whose metadata files are gone can not be dropped through Glue's Iceberg
# REST endpoint, which loads it first. The cloud CI runs this for the Iceberg tables its runs leave behind.
#
# Usage: bash scripts/drop_glue_iceberg_tables.sh s3://<bucket>/<prefix>
set -euo pipefail

prefix="${1:?usage: $0 s3://<bucket>/<prefix>}"
case "$prefix" in
s3://*/?*) ;;
*)
	echo "refusing to match a whole bucket or a non-S3 prefix: '$prefix'" >&2
	exit 1
	;;
esac

for database in $(aws glue get-databases --query 'DatabaseList[].Name' --output text); do
	tables=$(aws glue get-tables --database-name "$database" --output text \
		--query "TableList[?Parameters.metadata_location != null && starts_with(Parameters.metadata_location, '$prefix')].Name")
	for table in $tables; do
		echo "Deleting Glue table $database.$table"
		aws glue delete-table --database-name "$database" --name "$table"
	done
done
