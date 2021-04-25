
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object tier_segment_tracks extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().
    appName("weather").
    master("local[*]").
    getOrCreate()

  import spark.implicits._

  
  val inputdf = spark.
                read.
                format("json").
                option("mode", "DROPMALFORMED").
                option("path", "/Users/apatni/Downloads/Tier_Data_Engineering_Challenge/track_events.json").
                load()
                
  val new_columns = inputdf.
                     columns.map(x=> x.split('.')(1))
  
  val finaldf = inputdf.
                toDF(new_columns:_*)
                
                
  finaldf.show()
  
  finaldf.
  write.
  mode(SaveMode.Append).
  saveAsTable("segment_tracks")
  
  spark.close()
}