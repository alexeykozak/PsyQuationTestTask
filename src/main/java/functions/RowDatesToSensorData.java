package functions;

import model.SensorData;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class RowDatesToSensorData implements MapFunction<Row, SensorData> {
    @Override
    public SensorData call(Row row) {

        Timestamp timestamp = row.getTimestamp(1);
        String location = row.getString(0);

        SensorData sensorData = new SensorData();
        sensorData.setLocation(location);

        ZonedDateTime timeSlotStart = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime()), ZoneId.systemDefault());
        sensorData.setTimeSlotStart(timeSlotStart);

        return sensorData;
    }
}
