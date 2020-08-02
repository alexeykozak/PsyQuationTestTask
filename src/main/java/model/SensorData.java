package model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import utils.ZonedDateTimeDeserializer;

import java.io.Serializable;
import java.time.ZonedDateTime;

@Data
@JsonInclude
public class SensorData implements Serializable {
    @JsonProperty("TimeSlotStart")
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime timeSlotStart;
    @JsonProperty("Location")
    private String location;
    @JsonProperty("TempMin")
    private Double tempMin;
    @JsonProperty("TempMax")
    private Double tempMax;
    @JsonProperty("TempAvg")
    private Double tempAvg;
    @JsonProperty("TempCnt")
    private int tempCnt;
    @JsonProperty("Presence")
    private boolean presence;
    @JsonProperty("PresenceCnt")
    private int presenceCnt;
}
