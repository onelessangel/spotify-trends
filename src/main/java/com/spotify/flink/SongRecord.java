package com.spotify.flink;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SongRecord {
    private String spotifyId;
    private String name;
//    private String artists;
    private int dailyRank;
//    private int dailyMovement;
//    private int weeklyMovement;
    private String country;
    private LocalDate snapshotDate;
//    private int popularity;
    private boolean isExplicit;
    private long durationMs;
//    private String albumName;
//    private String albumReleaseDate;
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

    @Override
    public String toString() {
        return String.format("%s | Daily position: %d | %s (%s)", name, dailyRank, country, snapshotDate);
    }
}
