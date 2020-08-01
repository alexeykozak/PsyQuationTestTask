package functions;

import model.SensorData;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PrepareToReduce implements PairFunction<SensorData, String, SensorData> {

    @Override
    public Tuple2<String, SensorData> call(SensorData sensorData) throws Exception {
        String key = sensorData.getTimeSlotStart() + sensorData.getLocation();
        return new Tuple2<>(key, sensorData);
    }
}
