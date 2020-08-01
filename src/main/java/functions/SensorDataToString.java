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
        outputData.setTempMin(safeToString(s.getTempMin(), ""));
        outputData.setTempMax(safeToString(s.getTempMax(), ""));
        outputData.setTempAvg(safeToString(s.getTempAvg(), ""));
        outputData.setTempCnt(safeToString(s.getTempCnt(), "0"));
        outputData.setPresence(s.getPresence());
        outputData.setPresenceCnt(safeToString(s.getPresenceCnt(), "0"));

        return MAPPER.writeValueAsString(outputData);
    }

    private String safeToString(Object o, String nullValue) {
        if (o != null) {
            return o.toString();
        } else {
            return nullValue;
        }
    }
}
