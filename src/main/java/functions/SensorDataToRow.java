package functions;

import model.SensorData;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SensorDataToRow implements MapFunction<SensorData, Row> {
    @Override
    public Row call(SensorData sensorData) {
        StructType structType = new StructType()
                .add(DataTypes.createStructField("TimeSlotStart", DataTypes.TimestampType, false))
                .add(DataTypes.createStructField("Location", DataTypes.StringType, false))
                .add(DataTypes.createStructField("TempMin", DataTypes.DoubleType, false))
                .add(DataTypes.createStructField("TempMax", DataTypes.DoubleType, false))
                .add(DataTypes.createStructField("TempAvg", DataTypes.DoubleType, false))
                .add(DataTypes.createStructField("TempCnt", DataTypes.IntegerType, false))
                .add(DataTypes.createStructField("PresenceCnt", DataTypes.BooleanType, false))
                .add(DataTypes.createStructField("Presence", DataTypes.IntegerType, false));

        Row genericRowWithSchema = new GenericRowWithSchema(new Object[]{
                sensorData.getTimeSlotStart(),
                sensorData.getLocation(),
                sensorData.getTempMin(),
                sensorData.getTempMax(),
                sensorData.getTempAvg(),
                sensorData.getTempCnt(),
                sensorData.getPresenceCnt(),
                sensorData.isPresence()}, structType);
        return genericRowWithSchema;

    }
}
