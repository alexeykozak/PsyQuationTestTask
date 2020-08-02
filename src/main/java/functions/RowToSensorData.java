package functions;

import model.SensorData;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class RowToSensorData implements MapFunction<Row, SensorData> {
    @Override
    public SensorData call(Row row) {

        Timestamp timestamp = row.getTimestamp(2);
        String channelType = row.getString(4);
        String location = row.getString(5);

        SensorData sensorData = new SensorData();
        sensorData.setLocation(location);

        ZonedDateTime timeSlotStart = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime()), ZoneId.systemDefault());
        sensorData.setTimeSlotStart(timeSlotStart);

        if (channelType.equals("temperature")) {
            double temperature = (Double.parseDouble(row.getString(3)) - 32) / 1.8;
            sensorData.setTempCnt(1);
            sensorData.setTempAvg(temperature);
            sensorData.setTempMin(temperature);
            sensorData.setTempMax(temperature);
        } else if (channelType.equals("presence")) {
            int presence = Integer.parseInt(row.getString(3));
            if (presence > 0) {
                sensorData.setPresence(true);
                sensorData.setPresenceCnt(1);
            }
        }


        return sensorData;
    }
}
