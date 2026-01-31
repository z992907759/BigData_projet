import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex02DataIngestion")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    // 1. LECTURE (Source Minio Bronze)
    val rawDF = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2025-01.parquet")

    // 2. BRANCHEMENT ET NETTOYAGE (Branche 1 : ML Ready)
    val cleanedDF = rawDF
      // Respect du contrat NYC : suppression des anomalies
      .filter(
        $"passenger_count" > 0 &&
          $"trip_distance" > 0 &&
          $"fare_amount" > 0 &&
          $"tpep_pickup_datetime" < $"tpep_dropoff_datetime" // Dates valides
      )
      .na.drop(Seq("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"))
      .withColumn("source_file", lit("yellow_tripdata_2025-01.parquet"))

    // EXPORT BRANCHE 1 : Vers Minio (Datalake Silver)
    cleanedDF.write
      .mode(SaveMode.Overwrite)
      .parquet("s3a://nyc-cleaned/yellow_tripdata_2025-01-cleaned.parquet")

    // 3. TRANSFORMATION ET MAPPING (Branche 2 : DWH Postgres)
    // Configuration JDBC
    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres" // à vérifier
    val props = new Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "password")
    props.setProperty("driver", "org.postgresql.Driver")

    // Chargement / insertion des dimensions (on ajoute uniquement les nouvelles valeurs)

    // dim_vendor
    val existingVendor = spark.read.jdbc(jdbcUrl, "dwh.dim_vendor", props).select($"vendor_id")
    val newVendor = cleanedDF.select($"VendorID".cast("int").as("vendor_id")).na.drop().distinct()
      .join(existingVendor, Seq("vendor_id"), "left_anti")
    newVendor.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_vendor", props)

    // dim_rate_code
    val existingRate = spark.read.jdbc(jdbcUrl, "dwh.dim_rate_code", props).select($"rate_code_id")
    val newRate = cleanedDF.select($"RatecodeID".cast("int").as("rate_code_id")).na.drop().distinct()
      .join(existingRate, Seq("rate_code_id"), "left_anti")
    newRate.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_rate_code", props)

    // dim_payment_type
    val existingPay = spark.read.jdbc(jdbcUrl, "dwh.dim_payment_type", props).select($"payment_type_id")
    val newPay = cleanedDF.select($"payment_type".cast("int").as("payment_type_id")).na.drop().distinct()
      .join(existingPay, Seq("payment_type_id"), "left_anti")
    newPay.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_payment_type", props)

    // dim_location (PU + DO)
    val existingLoc = spark.read.jdbc(jdbcUrl, "dwh.dim_location", props).select($"location_id")
    val newLoc = cleanedDF
      .select($"PULocationID".cast("int").as("location_id"))
      .union(cleanedDF.select($"DOLocationID".cast("int").as("location_id")))
      .na.drop().distinct()
      .join(existingLoc, Seq("location_id"), "left_anti")
    newLoc.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_location", props)

    // dim_datetime (pickup + dropoff)
    val pickupTsDF = cleanedDF.select($"tpep_pickup_datetime".cast("timestamp").as("ts"))
    val dropoffTsDF = cleanedDF.select($"tpep_dropoff_datetime".cast("timestamp").as("ts"))
    val allTsDF = pickupTsDF.union(dropoffTsDF).na.drop().distinct()

    val existingTs = spark.read.jdbc(jdbcUrl, "dwh.dim_datetime", props).select($"ts")
    val newTs = allTsDF.join(existingTs, Seq("ts"), "left_anti")
      .withColumn("date", to_date($"ts"))
      .withColumn("year", year($"ts"))
      .withColumn("month", month($"ts"))
      .withColumn("day", dayofmonth($"ts"))
      .withColumn("hour", hour($"ts"))
      .withColumn("dow", dayofweek($"ts"))

    newTs.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_datetime", props)

    // Recharger les dimensions (avec surrogate keys)
    val dimVendor = spark.read.jdbc(jdbcUrl, "dwh.dim_vendor", props).select($"vendor_key", $"vendor_id")
    val dimRate = spark.read.jdbc(jdbcUrl, "dwh.dim_rate_code", props).select($"rate_code_key", $"rate_code_id")
    val dimPay = spark.read.jdbc(jdbcUrl, "dwh.dim_payment_type", props).select($"payment_type_key", $"payment_type_id")
    val dimLoc = spark.read.jdbc(jdbcUrl, "dwh.dim_location", props).select($"location_key", $"location_id")
    val dimDt = spark.read.jdbc(jdbcUrl, "dwh.dim_datetime", props).select($"datetime_key", $"ts")

    // Préparer la fact table avec natural keys
    val baseFact = cleanedDF
      .withColumn("vendor_id", $"VendorID".cast("int"))
      .withColumn("rate_code_id", $"RatecodeID".cast("int"))
      .withColumn("payment_type_id", $"payment_type".cast("int"))
      .withColumn("pu_location_id", $"PULocationID".cast("int"))
      .withColumn("do_location_id", $"DOLocationID".cast("int"))
      .withColumn("pickup_ts", $"tpep_pickup_datetime".cast("timestamp"))
      .withColumn("dropoff_ts", $"tpep_dropoff_datetime".cast("timestamp"))

    // Joins dimensions -> surrogate keys
    val withVendor = baseFact.join(dimVendor, Seq("vendor_id"), "left")
    val withRate = withVendor.join(dimRate, Seq("rate_code_id"), "left")
    val withPay2 = withRate.join(dimPay, Seq("payment_type_id"), "left")

    val puDim = dimLoc.withColumnRenamed("location_id", "pu_location_id")
      .withColumnRenamed("location_key", "pu_location_key")
    val doDim = dimLoc.withColumnRenamed("location_id", "do_location_id")
      .withColumnRenamed("location_key", "do_location_key")

    val withPuLoc = withPay2.join(puDim, Seq("pu_location_id"), "left")
    val withDoLoc = withPuLoc.join(doDim, Seq("do_location_id"), "left")

    val pickupDim = dimDt.withColumnRenamed("ts", "pickup_ts")
      .withColumnRenamed("datetime_key", "pickup_datetime_key")
    val dropoffDim = dimDt.withColumnRenamed("ts", "dropoff_ts")
      .withColumnRenamed("datetime_key", "dropoff_datetime_key")

    val withPickupKey = withDoLoc.join(pickupDim, Seq("pickup_ts"), "left")
    val withDropoffKey = withPickupKey.join(dropoffDim, Seq("dropoff_ts"), "left")

    // Mapping final pour correspondre au schéma fact_trip
    val factTripDF = withDropoffKey.select(
      $"pickup_datetime_key",
      $"dropoff_datetime_key",
      $"vendor_key",
      $"rate_code_key",
      $"payment_type_key",
      $"pu_location_key",
      $"do_location_key",
      $"passenger_count",
      $"trip_distance",
      $"fare_amount",
      $"extra",
      $"mta_tax",
      $"tip_amount",
      $"tolls_amount",
      $"improvement_surcharge",
      $"total_amount",
      $"source_file"
    )

    // EXPORT BRANCHE 2 : Vers PostgreSQL
    // On utilise Append pour ne pas supprimer les données des mois précédents
    factTripDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "dwh.fact_trip", props)

    println("✅ Job terminé : Branche 1 (Minio) et Branche 2 (Postgres) synchronisées.")
    spark.stop()
  }
}
