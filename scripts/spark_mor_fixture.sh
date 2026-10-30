#!/usr/bin/env bash
# Writes a merge-on-read Iceberg table with Apache Spark (the reference
# engine) for tests/iceberg.rs::reads_a_spark_merge_on_read_table.
#
#   scripts/spark_mor_fixture.sh <warehouse-dir>   # prints PLIMSOLL_ICEBERG_MOR=...
#
# The warehouse is mounted at the same absolute path inside the container, so
# the paths Spark records in the metadata are valid on the host.
set -euo pipefail

SPARK_IMAGE=apache/spark:3.5.5
ICEBERG_JAR=iceberg-spark-runtime-3.5_2.12-1.9.2.jar
JAR_URL=https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/$ICEBERG_JAR

wh=$(mkdir -p "$1" && cd "$1" && pwd)
jars=${PLIMSOLL_JAR_CACHE:-$HOME/.cache/plimsoll/jars}
mkdir -p "$jars"
[ -f "$jars/$ICEBERG_JAR" ] || curl -sSfL -o "$jars/$ICEBERG_JAR" "$JAR_URL"
rm -rf "$wh/db" && chmod 777 "$wh"

sql=$(mktemp)
cat > "$sql" <<'SQL'
CREATE TABLE local.db.mor (id BIGINT, name STRING, ts TIMESTAMP) USING iceberg
  PARTITIONED BY (days(ts))
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read');
INSERT INTO local.db.mor SELECT id, concat('n', id),
  timestamp '2024-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0)
  FROM range(0, 48);
DELETE FROM local.db.mor WHERE id % 3 = 0;
UPDATE local.db.mor SET name = 'updated' WHERE id = 1;
SQL
chmod 644 "$sql"

docker run --rm -e TZ=UTC -v "$wh:$wh" -v "$jars:/jars:ro" -v "$sql:/mor.sql:ro" "$SPARK_IMAGE" \
  /opt/spark/bin/spark-sql --jars "/jars/$ICEBERG_JAR" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf "spark.sql.catalog.local.warehouse=$wh" \
  --conf spark.sql.session.timeZone=UTC \
  -f /mor.sql >/dev/null 2>&1
rm -f "$sql"
echo "PLIMSOLL_ICEBERG_MOR=$wh/db/mor"
