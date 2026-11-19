#!/bin/sh
# Seeds a local Floci (https://floci.io, an AWS emulator) for test/sql/child_catalogs. moto has no catalogs below the
# Data Catalog of an account; Floci serves Glue's catalog calls and shows each S3 Tables bucket as a child catalog,
# as the S3 Tables integration of AWS does:
#   s3tablescatalog                      the federated catalog of the integration
#   s3tablescatalog/salesdemo            a table bucket, with the namespace 'retail' and the tables 'widget_sales'
#                                        (columns i, s) and 'pending' (no Iceberg metadata yet, so no columns)
#   s3tablescatalog/archive              another table bucket, also with retail.widget_sales (column archived_at)
#   fault_federation_down                a federated catalog without databases, whose GetDatabases fails behind
#   fault_catalogs_denied                scripts/glue_fault_proxy.py, as does GetCatalogs below this one
# and the S3 bucket 'glue-floci-test' for the Hive tables the tests create in the account's own catalog.
# Usage: scripts/floci_seed.sh [endpoint], default http://localhost:4566. Running it again changes nothing.
set -eu
ENDPOINT=${1:-http://localhost:4566}
ACCOUNT=000000000000
REGION=us-east-1
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_SESSION_TOKEN
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=$REGION AWS_REGION=$REGION
export AWS_EC2_METADATA_DISABLED=true
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

floci() {
	aws --endpoint-url "$ENDPOINT" --no-cli-pager --output text "$@"
}

# add_table <bucket> <table> [<iceberg fields json>]: without fields the table gets no metadata file
add_table() {
	arn=arn:aws:s3tables:$REGION:$ACCOUNT:bucket/$1
	floci s3tables create-table --table-bucket-arn "$arn" --namespace retail --name "$2" --format ICEBERG \
		>/dev/null 2>&1 || true
	if [ $# -lt 3 ]; then
		return
	fi
	if [ "$(floci s3tables get-table --table-bucket-arn "$arn" --namespace retail --name "$2" \
		--query metadataLocation)" != "None" ]; then
		return
	fi
	warehouse=$(floci s3tables get-table --table-bucket-arn "$arn" --namespace retail --name "$2" \
		--query warehouseLocation)
	token=$(floci s3tables get-table --table-bucket-arn "$arn" --namespace retail --name "$2" --query versionToken)
	cat >"$TMP/metadata.json" <<JSON
{"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "$warehouse",
 "last-sequence-number": 0, "last-updated-ms": 1700000000000, "last-column-id": 2, "current-schema-id": 0,
 "schemas": [{"type": "struct", "schema-id": 0, "fields": $3}],
 "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
 "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}], "properties": {},
 "current-snapshot-id": -1, "refs": {}, "snapshots": [], "statistics": [], "snapshot-log": [], "metadata-log": []}
JSON
	floci s3 cp "$TMP/metadata.json" "$warehouse/metadata/00001-seed.metadata.json" >/dev/null
	floci s3tables update-table-metadata-location --table-bucket-arn "$arn" --namespace retail --name "$2" \
		--version-token "$token" --metadata-location "$warehouse/metadata/00001-seed.metadata.json" >/dev/null
}

until floci sts get-caller-identity >/dev/null 2>&1; do
	echo 'waiting for Floci'
	sleep 1
done
floci s3 mb s3://glue-floci-test >/dev/null 2>&1 || true
floci glue create-catalog --cli-input-json '{"Name": "s3tablescatalog", "CatalogInput": {"FederatedCatalog":
	{"Identifier": "arn:aws:s3tables:'$REGION':'$ACCOUNT':bucket/*", "ConnectionName": "aws:s3tables"},
	"AllowFullTableExternalDataAccess": "True", "CreateDatabaseDefaultPermissions": [],
	"CreateTableDefaultPermissions": []}}' >/dev/null 2>&1 || true
for catalog in fault_federation_down fault_catalogs_denied; do
	floci glue create-catalog --cli-input-json '{"Name": "'$catalog'", "CatalogInput": {"FederatedCatalog":
		{"Identifier": "arn:aws:s3tables:'$REGION':'$ACCOUNT':bucket/no-such-bucket", "ConnectionName": "aws:s3tables"},
		"CreateDatabaseDefaultPermissions": [], "CreateTableDefaultPermissions": []}}' >/dev/null 2>&1 || true
done
for bucket in salesdemo archive; do
	floci s3tables create-table-bucket --name $bucket >/dev/null 2>&1 || true
	floci s3tables create-namespace --table-bucket-arn arn:aws:s3tables:$REGION:$ACCOUNT:bucket/$bucket \
		--namespace retail >/dev/null 2>&1 || true
done
add_table salesdemo widget_sales '[{"id": 1, "name": "i", "required": false, "type": "int"},
	{"id": 2, "name": "s", "required": false, "type": "string"}]'
add_table salesdemo pending
add_table archive widget_sales '[{"id": 1, "name": "archived_at", "required": false, "type": "string"}]'
echo "Floci at $ENDPOINT: s3tablescatalog with the table buckets salesdemo and archive, S3 bucket glue-floci-test"
