import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        Dataset<Row> ds1 = spark.read().csv("ds1.csv")
                .toDF("SensorId", "ChannelId", "ChannelType", "LocationId")
                .filter(col("ChannelType").isin("temperature", "battery", "presence"))
                .dropDuplicates("SensorId", "ChannelId");

        Dataset<Row> ds2 = spark.read().csv("ds2.csv")
                .toDF("SensorId", "ChannelId", "TimeStamp", "Value")
                .withColumn("TimeStamp", to_timestamp(col("TimeStamp")))
                .dropDuplicates("SensorId", "ChannelId", "TimeStamp");
        ds2.cache();

        Dataset<Row> join = ds2.join(ds1, toSeq("SensorId", "ChannelId"), "left")
                .na().drop()
                .cache();

        join = join.withColumn("TimeStamp", window(col("TimeStamp"), "15 minutes"));

        join.show(1000, false);
        join.printSchema();


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
