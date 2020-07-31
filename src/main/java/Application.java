import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Application {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.driver.bindAddress", "localhost")
                .appName("TestTask")
                .getOrCreate();

//        spark.range()

        Dataset<Row> ds1 = spark.read().csv("ds1.csv")
                .toDF("SensorId", "ChannelId", "ChannelType", "LocationId")
                .filter(col("ChannelType").isin("temperature", "battery", "presence"))
                .withColumn("LocationId", trim(col("LocationId")))
                .dropDuplicates("SensorId", "ChannelId");

        Dataset<Row> ds2 = spark.read().csv("ds2.csv")
                .toDF("SensorId", "ChannelId", "TimeStamp", "Value")
                .withColumn("TimeStamp", to_timestamp(col("TimeStamp")))
                .dropDuplicates("SensorId", "ChannelId", "TimeStamp");

        Dataset<Row> output1 = ds2.join(ds1, toSeq("SensorId", "ChannelId"), "left")
                .na().drop()
                .withColumn("TimeStamp", window(col("TimeStamp"), "15 minutes"))
                .withColumn("TimeSlotStart", col("TimeStamp.start"))
                .withColumn("Temperature", when(col("ChannelType").isin("temperature"), col("Value")))
                .withColumn("Temperature", expr("(temperature - 32 )/ 1.8"))
                .withColumn("Battery", when(col("ChannelType").isin("battery"), col("Value")))
                .withColumn("Presence", when(col("ChannelType").isin("presence"), col("Value")).cast(DataTypes.IntegerType))
                .drop("SensorId", "ChannelId", "Value", "ChannelType", "TimeStamp")
                .groupBy(col("TimeSlotStart"), col("LocationId"))
                .agg(
                        round(min("Temperature"), 2).alias("TempMin"),
                        round(max("Temperature"), 2).alias("TempMax"),
                        round(avg("Temperature"), 2).alias("TempAvg"),
                        count("Temperature").alias("TempCnt"),
                        sum(col("Presence")).alias("PresenceCnt"),
                        col("LocationId").alias("Location"))
                .withColumn("Presence", when(col("PresenceCnt").$greater(0), true).otherwise(false));


        output1 = output1.select(
                col("TimeSlotStart"),
                col("Location"),
                col("TempMin"),
                col("TempMax"),
                col("TempAvg"),
                col("TempCnt"),
                col("Presence"),
                col("PresenceCnt"))
                .orderBy(col("TimeSlotStart"), col("Location"));

        output1.repartition(1)
//                .filter(col("TimeSlotStart").isin("2018-03-23 14:00:00"))
                .write().json("output1.json");


//        try {
//            System.in.read();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        spark.stop();
    }


    @SafeVarargs
    private static <T> Seq<T> toSeq(T... values) {
        return JavaConverters.asScalaIteratorConverter(Arrays.stream(values).iterator()).asScala().toSeq();
    }
}
