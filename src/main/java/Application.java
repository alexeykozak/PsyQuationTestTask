import functions.*;
import model.SensorData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
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
        spark.sparkContext().hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

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
        Dataset<Row> datesRow = getDatesDataset(spark, startDate, endDate);

        Dataset<SensorData> dates = datesRow.map(new RowDatesToSensorData(), Encoders.javaSerialization(SensorData.class));

        Dataset<SensorData> output1 = ds2.join(ds1, toSeq("SensorId", "ChannelId"), "left")
                .withColumn("TimeStamp", window(col("TimeStamp"), "15 minutes"))
                .withColumn("TimeStamp", col("TimeStamp.start"))
                .na().drop()
                .map(new RowToSensorData(), Encoders.javaSerialization(SensorData.class))
                .union(dates)
                .groupByKey((MapFunction<SensorData, String>) sensorData -> sensorData.getTimeSlotStart().toString() + sensorData.getLocation(), Encoders.STRING())
                .reduceGroups(new AggregateData())
                .map((MapFunction<Tuple2<String, SensorData>, SensorData>) v1 -> v1._2, Encoders.javaSerialization(SensorData.class));

        output1
                .map(new SensorDataToString(), Encoders.STRING())
                .orderBy("value")
                .coalesce(1)
                .rdd()
                .saveAsTextFile(ds1OutputPath);

        JavaRDD<String> rdd = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(ds1OutputPath + "/part*");

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
                .withColumn("TimeSlotStart", from_unixtime(col("date")).cast(DataTypes.TimestampType))
                .withColumn("Location", col("Location"))
                .drop(col("date"));
    }


    @SafeVarargs
    private static <T> Seq<T> toSeq(T... values) {
        return JavaConverters.asScalaIteratorConverter(Arrays.stream(values).iterator()).asScala().toSeq();
    }
}
