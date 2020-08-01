package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.SensorData;
import org.apache.spark.api.java.function.Function;

public class StringToSensorData implements Function<String, SensorData> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public SensorData call(String s) throws Exception {
        return MAPPER.readValue(s, SensorData.class);
    }
}
