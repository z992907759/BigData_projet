import org.apache.spark.sql.SparkSession
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

object Main {
  def main(args: Array[String]): Unit = {

    val year  = if (args.length > 0) args(0) else "2025"
    val month = if (args.length > 1) args(1) else "01"

    val fileName  = s"yellow_tripdata_${year}-${month}.parquet"
    val localPath = s"data/raw/$fileName"
    val s3Path    = s"s3a://nyc-raw/$fileName"

    // EX1 automatisation: download the parquet automatically if it's missing locally
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

    val df = spark.read.parquet(localPath)

    df.printSchema()
    df.show(5)

    df.write
      .mode("overwrite")
      .parquet(s3Path)

    spark.stop()
  }
}