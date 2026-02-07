import org.apache.spark.sql.SparkSession
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

object Main {
  def main(args: Array[String]): Unit = {

    // Choose one mode:
    //   "SINGLE_MONTH"  -> one specific month
    //   "ONE_YEAR"      -> all 12 months for a given year
    //   "YEAR_RANGE"    -> all months for a range of years
    val MODE = "ONE_YEAR"

    // Used if MODE == "SINGLE_MONTH"
    val SINGLE_YEAR  = 2025
    val SINGLE_MONTH = 1

    // Used if MODE == "ONE_YEAR"
    val YEAR = 2024

    // Used if MODE == "YEAR_RANGE"
    val START_YEAR = 2023
    val END_YEAR   = 2025

    // Build the list of (year, month) pairs from configuration.
    // Default behavior: just run `sbt run` and edit the config above when needed.
    val yearMonthPairs: Seq[(Int, Int)] = MODE match {
      case "SINGLE_MONTH" =>
        require(SINGLE_MONTH >= 1 && SINGLE_MONTH <= 12, s"Invalid month: $SINGLE_MONTH")
        Seq((SINGLE_YEAR, SINGLE_MONTH))

      case "ONE_YEAR" =>
        (1 to 12).map(m => (YEAR, m))

      case "YEAR_RANGE" =>
        require(START_YEAR <= END_YEAR, s"Invalid year range: $START_YEAR-$END_YEAR")
        for {
          y <- START_YEAR to END_YEAR
          m <- 1 to 12
        } yield (y, m)

      case other =>
        throw new IllegalArgumentException(s"Unknown MODE: $other")
    }

    def fileNameFor(year: Int, month: Int): String = f"yellow_tripdata_${year}%04d-${month}%02d.parquet"
    def localPathFor(fileName: String): String = s"data/raw/$fileName"
    def s3PathFor(fileName: String): String = s"s3a://nyc-raw/$fileName"

    // EX1 automatisation: download parquet(s) automatically if missing locally
    yearMonthPairs.foreach { case (year, month) =>
      val fileName  = fileNameFor(year, month)
      val localPath = localPathFor(fileName)

      val localFile = Paths.get(localPath)
      if (!Files.exists(localFile)) {
        Files.createDirectories(localFile.getParent)
        val url = s"https://d37ci6vzurychx.cloudfront.net/trip-data/$fileName"
        println(s"[EX1] Downloading $url -> $localPath")
        val in = new URL(url).openStream()
        try {
          Files.copy(in, localFile, StandardCopyOption.REPLACE_EXISTING)
        } finally {
          in.close()
        }
        println(s"[EX1] Download completed: $localPath")
      } else {
        println(s"[EX1] Local file already exists: $localPath")
      }
    }

    val spark = SparkSession.builder()
      .appName("Ex01DataRetrieval")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    // Read & upload each selected month to MinIO
    yearMonthPairs.zipWithIndex.foreach { case ((year, month), idx) =>
      val fileName  = fileNameFor(year, month)
      val localPath = localPathFor(fileName)
      val s3Path    = s3PathFor(fileName)

      println(s"[EX1] Reading local parquet: $localPath")
      val df = spark.read.parquet(localPath)

      // Print schema/sample only once to avoid noisy logs when doing many months
      if (idx == 0) {
        df.printSchema()
        df.show(5)
      }

      println(s"[EX1] Writing to MinIO: $s3Path")
      df.write
        .mode("overwrite")
        .parquet(s3Path)
    }

    spark.stop()
  }
}