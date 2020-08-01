package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.SensorData;
import model.SensorOutputData;
import org.apache.spark.api.java.function.Function;

public class SensorDataToString implements Function<SensorData, String> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String call(SensorData s) throws Exception {
        SensorOutputData outputData = new SensorOutputData();

        outputData.setTimeSlotStart(s.getTimeSlotStart());
        outputData.setLocation(s.getLocation());
        outputData.setTempMin(safeToString(s.getTempMin()));
        outputData.setTempMax(safeToString(s.getTempMax()));
        outputData.setTempAvg(safeToString(s.getTempAvg()));
        outputData.setTempCnt(s.getTempCnt());
        outputData.setPresence(s.getPresence());
        outputData.setPresenceCnt(s.getPresenceCnt());

        return MAPPER.writeValueAsString(outputData);
    }

    private String safeToString(Object o) {
        if (o != null) {
            return o.toString();
        } else {
            return "";
        }
    }
}
