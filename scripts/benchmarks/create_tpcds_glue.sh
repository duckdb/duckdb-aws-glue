#!/usr/bin/env bash
# Generates TPC-DS with dsdgen and writes it as parquet Hive tables registered in a live AWS Glue catalog, twice:
# partitioned (fact tables by their date key, one partition per day) and unpartitioned.
#
#   AWS_PROFILE=... scripts/benchmarks/create_tpcds_glue.sh
#
# Needs a release build (make release): writing a NULL partition more than one batch at a time trips a D_ASSERT in
# DuckDB's partitioned copy (see benchmark/tpcds/tpcds.benchmark.in). Tables that already exist in Glue are skipped,
# so a failed run can be restarted; a failed CTAS removes its own files and table.
set -euo pipefail

GLUE_CATALOG_ID="${GLUE_CATALOG_ID:-176760123605}"
SF="${SF:-100}"
REGION="${REGION:-us-east-1}"
BUCKET_ROOT="${BUCKET_ROOT:-s3://duckdb-aws-glue-benchmark-data/tpcds}"
PARTITIONED_ROOT="${PARTITIONED_ROOT:-${BUCKET_ROOT}/partitioned/sf${SF}}"
UNPARTITIONED_ROOT="${UNPARTITIONED_ROOT:-${BUCKET_ROOT}/unpartitioned/sf${SF}}"
PARTITIONED_DB="${PARTITIONED_DB:-tpcds_sf${SF}_partitioned}"
UNPARTITIONED_DB="${UNPARTITIONED_DB:-tpcds_sf${SF}_unpartitioned}"
WORK_DIR="${WORK_DIR:-$(pwd)/duckdb_benchmark_data}"
BUILD_DIR="${BUILD_DIR:-$(git rev-parse --show-toplevel)/build/release}"
DUCKDB="${DUCKDB:-${BUILD_DIR}/duckdb}"
MEMORY_LIMIT="${MEMORY_LIMIT:-40GB}"
WHICH="${WHICH:-partitioned unpartitioned}"
SECRET_SQL="${SECRET_SQL:-CREATE SECRET (TYPE S3, PROVIDER credential_chain, REGION '${REGION}');}"
ATTACH_OPTIONS="${ATTACH_OPTIONS:-}"

FACT_TABLES="store_sales:ss_sold_date_sk store_returns:sr_returned_date_sk catalog_sales:cs_sold_date_sk
catalog_returns:cr_returned_date_sk web_sales:ws_sold_date_sk web_returns:wr_returned_date_sk inventory:inv_date_sk"
DIMENSION_TABLES="call_center catalog_page customer customer_address customer_demographics date_dim
household_demographics income_band item promotion reason ship_mode store time_dim warehouse web_page web_site"

mkdir -p "${WORK_DIR}/tmp"
SOURCE_DB="${WORK_DIR}/tpcds_sf${SF}.duckdb"

LOAD_SQL=""
for extension in parquet httpfs aws tpcds glue; do
	LOAD_SQL+="LOAD '${BUILD_DIR}/extension/${extension}/${extension}.duckdb_extension'; "
done

run_sql() {
	"${DUCKDB}" -no-agent -unsigned -bail -cmd "${LOAD_SQL}" "$@"
}

if [ ! -f "${SOURCE_DB}.done" ]; then
	echo "generating TPC-DS sf${SF} into ${SOURCE_DB}"
	rm -f "${SOURCE_DB}" "${SOURCE_DB}.wal"
	run_sql "${SOURCE_DB}" -c "SET memory_limit = '${MEMORY_LIMIT}'; SET temp_directory = '${WORK_DIR}/tmp';
		CALL dsdgen(sf = ${SF});"
	touch "${SOURCE_DB}.done"
fi

attach_sql() {
	cat <<-EOF
		SET memory_limit = '${MEMORY_LIMIT}';
		SET temp_directory = '${WORK_DIR}/tmp';
		SET preserve_insertion_order = false;
		SET aws_default_credentials = false;
		${SECRET_SQL}
		ATTACH '${SOURCE_DB}' AS src (READ_ONLY);
		ATTACH '${GLUE_CATALOG_ID}' AS glue (TYPE glue, REGION '${REGION}'${ATTACH_OPTIONS});
	EOF
}

ctas() {
	local db="$1" root="$2" table="$3" partition_key="${4:-}"
	local partitioned_by=""
	if [ -n "${partition_key}" ]; then
		partitioned_by="PARTITIONED BY (${partition_key})"
	fi
	echo "${db}.${table}" >&2
	run_sql -c "$(attach_sql)
		CREATE TABLE IF NOT EXISTS glue.${db}.${table} ${partitioned_by}
			WITH (format = 'parquet', location = '${root}/${table}')
			AS SELECT * FROM src.main.${table};"
}

load() {
	local kind="$1" db="$2" root="$3"
	run_sql -c "$(attach_sql)
		CREATE SCHEMA IF NOT EXISTS glue.${db} WITH (location = '${root}',
			comment = 'TPC-DS sf${SF} (${kind}), created by scripts/benchmarks/create_tpcds_glue.sh');"
	for entry in ${FACT_TABLES}; do
		local table="${entry%%:*}" key="${entry##*:}"
		if [ "${kind}" = partitioned ]; then
			ctas "${db}" "${root}" "${table}" "${key}"
		else
			ctas "${db}" "${root}" "${table}"
		fi
	done
	for table in ${DIMENSION_TABLES}; do
		ctas "${db}" "${root}" "${table}"
	done
}

for kind in ${WHICH}; do
	case "${kind}" in
	partitioned) load partitioned "${PARTITIONED_DB}" "${PARTITIONED_ROOT}" ;;
	unpartitioned) load unpartitioned "${UNPARTITIONED_DB}" "${UNPARTITIONED_ROOT}" ;;
	*) echo "unknown WHICH entry: ${kind}" >&2 && exit 1 ;;
	esac
done

for kind in ${WHICH}; do
	db="${PARTITIONED_DB}"
	if [ "${kind}" = unpartitioned ]; then
		db="${UNPARTITIONED_DB}"
	fi
	echo "row counts in glue.${db}:"
	run_sql -c "$(attach_sql)
		SELECT 'store_sales' AS t, count(*) FROM glue.${db}.store_sales
		UNION ALL SELECT 'inventory', count(*) FROM glue.${db}.inventory
		UNION ALL SELECT 'date_dim', count(*) FROM glue.${db}.date_dim;"
done
