package functions;

import model.SensorData;
import org.apache.spark.api.java.function.Function;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class TruncateTime implements Function<SensorData, SensorData> {
    @Override
    public SensorData call(SensorData sensorData) {
        ZonedDateTime timeSlotStart = sensorData.getTimeSlotStart();
        ZonedDateTime truncated = timeSlotStart.truncatedTo(ChronoUnit.HOURS);

        sensorData.setTimeSlotStart(truncated);

        return sensorData;
    }
}
