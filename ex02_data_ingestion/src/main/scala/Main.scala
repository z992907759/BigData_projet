import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex02DataIngestion")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9001")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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

    // Mapping des colonnes pour correspondre au schéma fact_trip
    val factTripDF = cleanedDF.select(
      $"VendorID".as("vendor_key"), // Note : Idéalement il faudrait faire un join pour avoir la vraie SERIAL key 
      $"RatecodeID".as("rate_code_key"),
      $"payment_type".as("payment_type_key"),
      $"PULocationID".as("pu_location_key"),
      $"DOLocationID".as("do_location_key"),
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
      // Les datetime keys nécessitent normalement une transformation vers des IDs de dim_datetime
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
