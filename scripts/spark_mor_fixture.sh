#!/usr/bin/env bash
# Writes merge-on-read Iceberg tables with Apache Spark and Iceberg's Java
# writers (the reference implementation) for tests/iceberg.rs:
# position deletes (reads_a_spark_merge_on_read_table) and equality deletes
# (reads_reference_equality_deletes), v3 deletion vectors
# (reads_reference_deletion_vectors), and schema evolution
# (reads_an_evolved_spark_table).
#
#   scripts/spark_mor_fixture.sh <warehouse-dir>
#   # prints PLIMSOLL_ICEBERG_{MOR,EQ,EVOLVED,DV}=...
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

scala=$(mktemp --suffix=.scala)
cat > "$scala" <<'SCALA'
import org.apache.iceberg.{FileFormat, PartitionKey}
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord}
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.spark.Spark3Util

try {
  // Position deletes (ADR 0014): Spark's own DELETE / UPDATE in MOR mode.
  Seq(
    """CREATE TABLE local.db.mor (id BIGINT, name STRING, ts TIMESTAMP) USING iceberg
       PARTITIONED BY (days(ts))
       TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
                      'write.update.mode'='merge-on-read')""",
    """INSERT INTO local.db.mor SELECT id, concat('n', id),
       timestamp '2024-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0)
       FROM range(0, 48)""",
    // Refs (ADR 0029): a tag on the insert, and a branch with a row that
    // only exists there. Main's history (snapshot-log) is unaffected.
    "ALTER TABLE local.db.mor CREATE TAG after_insert",
    "ALTER TABLE local.db.mor CREATE BRANCH audit",
    "INSERT INTO local.db.mor.branch_audit VALUES (1000, 'audit', timestamp '2024-01-03 00:00:00')",
    "DELETE FROM local.db.mor WHERE id % 3 = 0",
    "UPDATE local.db.mor SET name = 'updated' WHERE id = 1"
  ).foreach(spark.sql)

  // Equality deletes (ADR 0016), as Flink upserts write them: keyed on
  // `city`, scoped to partition region='a', including a null key. Spark
  // cannot write them, so use Iceberg's own Java writer.
  Seq(
    """CREATE TABLE local.db.eq (id BIGINT, city STRING, region STRING) USING iceberg
       PARTITIONED BY (region) TBLPROPERTIES ('format-version'='2')""",
    """INSERT INTO local.db.eq VALUES (1, 'x', 'a'), (2, 'y', 'a'), (3, NULL, 'a'),
       (4, 'x', 'b'), (5, NULL, 'b')"""
  ).foreach(spark.sql)
  val t = Spark3Util.loadIcebergTable(spark, "local.db.eq")
  val keySchema = t.schema().select("city")
  val factory = new GenericAppenderFactory(t.schema(), t.spec(),
    Array(t.schema().findField("city").fieldId()), keySchema, null)
  val row = GenericRecord.create(t.schema())
  row.setField("region", "a")
  val partition = new PartitionKey(t.spec(), t.schema())
  partition.partition(row)
  val out = t.io().newOutputFile(t.locationProvider().newDataLocation(t.spec(), partition, "eq-a.parquet"))
  val writer = factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, partition)
  for (city <- Seq("x", null)) {
    val key = GenericRecord.create(keySchema)
    key.setField("city", city)
    writer.write(key)
  }
  writer.close()
  t.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
  // Newer than the delete: must survive although city = 'x'.
  spark.sql("INSERT INTO local.db.eq VALUES (6, 'x', 'a')")

  // Format v3: deletes become deletion vectors (Puffin blobs). Two deletes
  // on the same file so the second DV must replace the first.
  Seq(
    """CREATE TABLE local.db.dv (id BIGINT, name STRING) USING iceberg
       TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
                      'write.update.mode'='merge-on-read')""",
    "INSERT INTO local.db.dv SELECT id, concat('n', id) FROM range(0, 100000)",
    "DELETE FROM local.db.dv WHERE id % 7 = 0",
    "DELETE FROM local.db.dv WHERE id < 10",
    "UPDATE local.db.dv SET name = 'updated' WHERE id = 99999"
  ).foreach(spark.sql)
  val dv = spark.sql("SELECT count(*), sum(id) FROM local.db.dv").collect()(0)
  println(s"DV-CHECK ${dv.getLong(0)} ${dv.getLong(1)}")

  // Schema evolution (ADR 0040): an equality delete keyed on `town`, then
  // `town` renamed to `city`, `score` promoted int -> bigint, and a new
  // column re-using the name `town` (a new field id). Old files and the
  // delete only know the old names; Plimsoll must resolve by field id.
  Seq(
    """CREATE TABLE local.db.evolved (id BIGINT, town STRING, score INT) USING iceberg
       TBLPROPERTIES ('format-version'='2')""",
    "INSERT INTO local.db.evolved VALUES (1, 'lisbon', 10), (2, 'porto', 20), (4, 'faro', 40)"
  ).foreach(spark.sql)
  val ev = Spark3Util.loadIcebergTable(spark, "local.db.evolved")
  val evKey = ev.schema().select("town")
  val evFactory = new GenericAppenderFactory(ev.schema(), ev.spec(),
    Array(ev.schema().findField("town").fieldId()), evKey, null)
  val evOut = ev.io().newOutputFile(ev.locationProvider().newDataLocation("eq-town.parquet"))
  val evWriter = evFactory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(evOut), FileFormat.PARQUET, null)
  val porto = GenericRecord.create(evKey)
  porto.setField("town", "porto")
  evWriter.write(porto)
  evWriter.close()
  ev.newRowDelta().addDeletes(evWriter.toDeleteFile()).commit()
  Seq(
    "ALTER TABLE local.db.evolved RENAME COLUMN town TO city",
    "ALTER TABLE local.db.evolved ALTER COLUMN score TYPE BIGINT",
    "ALTER TABLE local.db.evolved ADD COLUMN town STRING",
    "INSERT INTO local.db.evolved VALUES (3, 'braga', 3000000000, 'new')"
  ).foreach(spark.sql)
  val evolved = spark.sql("SELECT id, city, score, town FROM local.db.evolved ORDER BY id")
    .collect().map(_.toString).mkString(";")
  require(evolved == "[1,lisbon,10,null];[3,braga,3000000000,new];[4,faro,40,null]",
    s"Spark reads $evolved")

  // The fixture is only useful if the reference engine agrees with it.
  val ids = spark.sql("SELECT id FROM local.db.eq ORDER BY id").collect().map(_.getLong(0)).toSeq
  require(ids == Seq(2L, 4L, 5L, 6L), s"Spark reads $ids")
  System.exit(0)
} catch { case e: Throwable => e.printStackTrace(); System.exit(1) }
SCALA
chmod 644 "$scala"

docker run --rm -e TZ=UTC -v "$wh:$wh" -v "$jars:/jars:ro" -v "$scala:/fixture.scala:ro" "$SPARK_IMAGE" \
  /opt/spark/bin/spark-shell --jars "/jars/$ICEBERG_JAR" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf "spark.sql.catalog.local.warehouse=$wh" \
  --conf spark.sql.session.timeZone=UTC \
  -i /fixture.scala >&2
rm -f "$scala"
echo "PLIMSOLL_ICEBERG_MOR=$wh/db/mor"
echo "PLIMSOLL_ICEBERG_EQ=$wh/db/eq"
echo "PLIMSOLL_ICEBERG_EVOLVED=$wh/db/evolved"
echo "PLIMSOLL_ICEBERG_DV=$wh/db/dv"
