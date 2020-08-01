package functions;

import model.SensorData;
import org.apache.spark.api.java.function.Function2;

public class AggregateData implements Function2<SensorData, SensorData, SensorData> {

    @Override
    public SensorData call(SensorData data1, SensorData data2) {
        if (data1.getTempCnt() > 0 &&
                data1.getTempCnt() > 0) {
            mergeTemp(data1, data2);
        } else if (data2.getTempCnt() > 0) {
            moveTemp(data2, data1);
        }

        Integer presence1Cnt = data1.getPresenceCnt();
        Integer presence2Cnt = data2.getPresenceCnt();
        int totalPresenceCnt = presence1Cnt + presence2Cnt;
        data1.setPresenceCnt(totalPresenceCnt);

        data1.setPresence(totalPresenceCnt > 0);

        return data1;
    }


    private void moveTemp(SensorData from, SensorData to) {
        to.setTempMax(from.getTempMax());
        to.setTempMin(from.getTempMin());
        to.setTempAvg(from.getTempAvg());
        to.setTempCnt(from.getTempCnt());
    }

    private void mergeTemp(SensorData data1, SensorData data2) {
        Double temp1Max = data1.getTempMax();
        Double temp2Max = data2.getTempMax();
        data1.setTempMax(temp2Max > temp1Max ? temp2Max : temp1Max);

        Double temp1Min = data1.getTempMin();
        Double temp2Min = data2.getTempMin();
        data1.setTempMin(temp2Min < temp1Min ? temp2Min : temp1Min);

        Integer temp1Cnt = data1.getTempCnt();
        Integer temp2Cnt = data2.getTempCnt();

        Double temp1Avg = data1.getTempAvg();
        Double temp2Avg = data2.getTempAvg();
        int totalTempCnt = temp1Cnt + temp2Cnt;

        double tempAvg = (temp1Avg * temp1Cnt + temp2Avg * temp2Cnt) / (totalTempCnt);
        double roundAvg = Math.round(tempAvg * 100.0) / 100.0;

        data1.setTempAvg(roundAvg);
        data1.setTempCnt(totalTempCnt);
    }
}
