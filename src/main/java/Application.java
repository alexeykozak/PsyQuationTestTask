import functions.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Application {
    private static final String DATE_FROM = "spark.app.custom.date.from";
    private static final String DATE_TO = "spark.app.custom.date.to";
    private static final String INPUT_DS1_PATH = "spark.app.custom.input.ds1.path";
    private static final String INPUT_DS2_PATH = "spark.app.custom.input.ds2.path";
    private static final String OUTPUT_DS1_PATH = "spark.app.custom.output.ds1.path";
    private static final String OUTPUT_DS2_PATH = "spark.app.custom.output.ds2.path";

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("TestTask")
                .getOrCreate();

        String dateFromString = spark.conf().get(DATE_FROM, "2018-03-21");
        String dateToString = spark.conf().get(DATE_TO, "2018-03-24");
        String ds1InputPath = spark.conf().get(INPUT_DS1_PATH, "ds1.csv");
        String ds2InputPath = spark.conf().get(INPUT_DS2_PATH, "ds2.csv");
        String ds1OutputPath = spark.conf().get(OUTPUT_DS1_PATH, "output1");
        String ds2OutputPath = spark.conf().get(OUTPUT_DS2_PATH, "output2");

        ZonedDateTime startDate = LocalDate.parse(dateFromString).atStartOfDay(ZoneId.systemDefault());
        ZonedDateTime endDate = LocalDate.parse(dateToString).atStartOfDay(ZoneId.systemDefault());

        if (!startDate.isBefore(endDate)) {
            throw new RuntimeException("Start time should be before end time");
        }

        Dataset<Row> ds1 = getMetaDataset(spark, ds1InputPath);
        Dataset<Row> ds2 = getDataDataset(spark, ds2InputPath);
        Dataset<Row> dates = getDatesDataset(spark, startDate, endDate);

        Dataset<Row> output1 = ds2.join(ds1, toSeq("SensorId", "ChannelId"), "left")
                .withColumn("TimeStamp", window(col("TimeStamp"), "15 minutes"))
                .withColumn("TimeSlotStart", col("TimeStamp.start"))
                .withColumn("Temperature", when(col("ChannelType").isin("temperature"), col("Value")))
                .withColumn("Temperature", (col("Temperature").minus(32)).divide(1.8)) //Convert co Celsius
                .withColumn("Battery", when(col("ChannelType").isin("battery"), col("Value")))
                .withColumn("Presence", when(col("ChannelType").isin("presence"), col("Value")).cast(DataTypes.IntegerType))
                .withColumn("Location", col("LocationId"))
                .drop("SensorId", "ChannelId", "Value", "ChannelType", "TimeStamp", "LocationId")
                .groupBy(col("TimeSlotStart"), col("Location"))
                .agg(
                        round(min("Temperature"), 2).alias("TempMin"),
                        round(max("Temperature"), 2).alias("TempMax"),
                        round(avg("Temperature"), 2).alias("TempAvg"),
                        count("Temperature").alias("TempCnt"),
                        sum(col("Presence")).alias("PresenceCnt")
                );


        output1 = dates
                .join(output1, toSeq("TimeSlotStart", "Location"), "left_outer")

                .withColumn("PresenceCnt", when(col("PresenceCnt").isNull(), 0).otherwise(col("PresenceCnt")))
                .withColumn("TempMin", when(col("TempMin").isNull(), "").otherwise(col("TempMin")))
                .withColumn("TempMax", when(col("TempMax").isNull(), "").otherwise(col("TempMax")))
                .withColumn("TempAvg", when(col("TempAvg").isNull(), "").otherwise(col("TempAvg")))
                .withColumn("TempCnt", when(col("TempCnt").isNull(), 0).otherwise(col("TempCnt")))
                .withColumn("Presence", when(col("PresenceCnt").$greater(0), true).otherwise(false));

        //set correct column order
        output1 = output1.select(
                col("TimeSlotStart"),
                col("Location"),
                col("TempMin"),
                col("TempMax"),
                col("TempAvg"),
                col("TempCnt"),
                col("Presence"),
                col("PresenceCnt"))
                .cache();

        output1
                .orderBy("TimeSlotStart", "Location")
                .coalesce(1)
                .write()
                .mode("overwrite")
                .json(ds1OutputPath);


        JavaRDD<String> rdd = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(ds1OutputPath + "/*.json");

        rdd
                .map(new StringToSensorData())
                .map(new TruncateTime())
                .mapToPair(new PrepareToReduce())
                .reduceByKey(new AggregateData())
                .sortByKey()
                .map(Tuple2::_2)
                .map(new SensorDataToString())
                .coalesce(1)
                .saveAsTextFile(ds2OutputPath);
    }

    private static Dataset<Row> getDataDataset(SparkSession spark, String inputPath) {
        return spark.read().csv(inputPath)
                .toDF("SensorId", "ChannelId", "TimeStamp", "Value")
                .withColumn("TimeStamp", to_timestamp(col("TimeStamp")))
                .dropDuplicates("SensorId", "ChannelId", "TimeStamp");
    }

    private static Dataset<Row> getMetaDataset(SparkSession spark, String inputPath) {
        return spark.read().csv(inputPath)
                .toDF("SensorId", "ChannelId", "ChannelType", "LocationId")
                .filter(col("ChannelType").isin("temperature", "battery", "presence"))
                .withColumn("LocationId", trim(col("LocationId")))
                .dropDuplicates("SensorId", "ChannelId");
    }

    private static Dataset<Row> getDatesDataset(SparkSession spark, ZonedDateTime startDate, ZonedDateTime endDate) {
        Dataset<Row> dates = spark.range(startDate.toEpochSecond(), endDate.toEpochSecond(), 60 * 15).toDF("date");

        List<Row> list = Arrays.asList(
                RowFactory.create("Room 0"),
                RowFactory.create("Room 1"),
                RowFactory.create("Room 2"));

        StructType structType = new StructType()
                .add(DataTypes.createStructField("Location", DataTypes.StringType, false));

        Dataset<Row> rooms = spark.createDataFrame(list, structType);

        return dates.crossJoin(rooms)
                .withColumn("TimeSlotStart", from_unixtime(col("date"))).drop(col("date"))
                .withColumn("Location", col("Location"));
    }


    @SafeVarargs
    private static <T> Seq<T> toSeq(T... values) {
        return JavaConverters.asScalaIteratorConverter(Arrays.stream(values).iterator()).asScala().toSeq();
    }
}
