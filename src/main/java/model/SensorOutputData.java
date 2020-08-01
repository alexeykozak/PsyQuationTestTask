package model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import utils.ZonedDateTimeSerializer;

import java.io.Serializable;
import java.time.ZonedDateTime;

@Data
@JsonInclude
@JsonPropertyOrder({
        "TimeSlotStart",
        "Location",
        "TempMin",
        "TempMax",
        "TempAvg",
        "TempCnt",
        "Presence",
        "PresenceCnt"
})
public class SensorOutputData implements Serializable {
    @JsonProperty("TimeSlotStart")
    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    private ZonedDateTime timeSlotStart;
    @JsonProperty("Location")
    private String location;
    @JsonProperty("TempMin")
    private String tempMin;
    @JsonProperty("TempMax")
    private String tempMax;
    @JsonProperty("TempAvg")
    private String tempAvg;
    @JsonProperty("TempCnt")
    private String tempCnt;
    @JsonProperty("Presence")
    private Boolean presence;
    @JsonProperty("PresenceCnt")
    private String presenceCnt;
}
