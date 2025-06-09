package com.spotify.flink.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SongRecordExtended extends SongRecord {
    private int consecutiveHitDaysCount;
    private List<String> consecutiveHitDaysList;
    private boolean isHit;

    public SongRecordExtended(SongRecord record, boolean isHit) {
        super(record);
        this.consecutiveHitDaysCount = 0;
        this.consecutiveHitDaysList = new ArrayList<>();
        this.isHit = isHit;
    }

    @Override
    public String toString() {
        return String.format("%s | %s | %d days: %s", getCountry(), getName(), consecutiveHitDaysCount, consecutiveHitDaysList);
    }
}
