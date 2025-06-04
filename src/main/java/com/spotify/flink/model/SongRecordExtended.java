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
public class SongRecordExtended {
    private String spotifyId;
    private String name;
    private int dailyRank;
    private String country;
    private LocalDate snapshotDate;
    private boolean isExplicit;
    private long durationMs;
    private double danceability;
    private double energy;
    private int key;
    private double loudness;
    private int mode;
    private double speechiness;
    private double acousticness;
    private double instrumentalness;
    private double liveness;
    private double valence;
    private double tempo;
    private int timeSignature;
    private int consecutiveHitDaysCount;
    private List<String> consecutiveHitDaysList;

    public SongRecordExtended(SongRecord record) {
        this.spotifyId = record.getSpotifyId();
        this.name = record.getName();
        this.dailyRank = record.getDailyRank();
        this.country = record.getCountry();
        this.snapshotDate = record.getSnapshotDate();
        this.isExplicit = record.isExplicit();
        this.durationMs = record.getDurationMs();
        this.danceability = record.getDanceability();
        this.energy = record.getEnergy();
        this.key = record.getKey();
        this.loudness = record.getLoudness();
        this.mode = record.getMode();
        this.speechiness = record.getSpeechiness();
        this.acousticness = record.getAcousticness();
        this.instrumentalness = record.getInstrumentalness();
        this.liveness = record.getLiveness();
        this.valence = record.getValence();
        this.tempo = record.getTempo();
        this.timeSignature = record.getTimeSignature();
        this.consecutiveHitDaysCount = 0;
        this.consecutiveHitDaysList = new ArrayList<>();
    }

    @Override
    public String toString() {
        return String.format("%s | %s | %d days: %s", country, name, consecutiveHitDaysCount, consecutiveHitDaysList);
    }
}
