package com.spotify.flink.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SongRecord {
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

    public SongRecord(SongRecord other) {
        this.spotifyId = other.spotifyId;
        this.name = other.name;
        this.dailyRank = other.dailyRank;
        this.country = other.country;
        this.snapshotDate = other.snapshotDate;
        this.isExplicit = other.isExplicit;
        this.durationMs = other.durationMs;
        this.danceability = other.danceability;
        this.energy = other.energy;
        this.key = other.key;
        this.loudness = other.loudness;
        this.mode = other.mode;
        this.speechiness = other.speechiness;
        this.acousticness = other.acousticness;
        this.instrumentalness = other.instrumentalness;
        this.liveness = other.liveness;
        this.valence = other.valence;
        this.tempo = other.tempo;
        this.timeSignature = other.timeSignature;
    }


    @Override
    public String toString() {
        return String.format("%s | Daily position: %d | %s (%s)", name, dailyRank, country, snapshotDate);
    }
}
