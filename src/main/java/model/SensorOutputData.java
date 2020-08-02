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
    @JsonProperty
    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    private ZonedDateTime timeSlotStart;
    @JsonProperty
    private String location;
    @JsonProperty
    private String tempMin;
    @JsonProperty
    private String tempMax;
    @JsonProperty
    private String tempAvg;
    @JsonProperty
    private int tempCnt;
    @JsonProperty
    private boolean presence;
    @JsonProperty
    private int presenceCnt;
}
