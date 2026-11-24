#!/usr/bin/env bash
# Writes the per-query TPC-DS benchmark files that only name a query: the SF100 execution benchmarks on the AWS Glue
# catalog (with the SF100 answer where DuckDB has one) and the planning benchmarks on it and on the local Glue (moto).
# The query text is read from duckdb/extension/tpcds/dsdgen/queries when they run. Run from the repository root after
# the duckdb submodule moves; CI checks that the files are up to date.
set -euo pipefail

QUERIES=duckdb/extension/tpcds/dsdgen/queries
ANSWERS=duckdb/extension/tpcds/dsdgen/answers/sf100

write_execution() {
	local directory=benchmark/tpcds/cloud_sf100/partitioned
	rm -f "${directory}"/q*.benchmark
	for file in "${QUERIES}"/*.sql; do
		local query
		query=$(basename "${file}" .sql)
		{
			echo "# name: ${directory}/q${query}.benchmark"
			echo "# description: Run query ${query} from the TPC-DS benchmark at SF100 on the partitioned parquet tables in the AWS Glue catalog"
			echo "# group: [partitioned]"
			echo
			echo "include benchmark/tpcds/cloud_sf100/tpcds_cloud_glue.benchmark.in"
			echo
			echo "name DSQ${query}_cloud_sf100_partitioned"
			echo "group tpcds_glue_cloud"
			echo "subgroup sf100_partitioned"
			echo
			echo "run ${QUERIES}/${query}.sql"
			if [ -f "${ANSWERS}/${query}.csv" ]; then
				echo
				echo "result ${ANSWERS}/${query}.csv"
			fi
		} > "${directory}/q${query}.benchmark"
	done
}

write_planning() {
	local directory="$1" template="$2" description="$3"
	rm -f "${directory}"/q*.benchmark
	for file in "${QUERIES}"/*.sql; do
		local query
		query=$(basename "${file}" .sql)
		{
			echo "# name: ${directory}/q${query}.benchmark"
			echo "# description: Plan (EXPLAIN) query ${query} from the TPC-DS benchmark ${description}"
			echo "# group: [$(basename "${directory}")]"
			echo
			echo "template ${directory}/${template}"
			echo "QUERY_NUMBER_PADDED=${query}"
		} > "${directory}/q${query}.benchmark"
	done
}

write_execution
write_planning benchmark/tpcds/planning/cloud_sf100 tpcds_cloud_planning.benchmark.in \
	"at SF100 on the partitioned parquet tables in the AWS Glue catalog"
write_planning benchmark/tpcds/planning/local_sf1 tpcds_local_planning.benchmark.in \
	"at SF1 on daily-partitioned parquet tables in the local Glue (moto) + S3"
