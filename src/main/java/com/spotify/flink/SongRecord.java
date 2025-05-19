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
    private int durationMs;
//    private String albumName;
//    private String albumReleaseDate;
    private float danceability;
    private float energy;
    private int key;
    private float loudness;
    private int mode;
    private float speechiness;
    private float acousticness;
    private float instrumentalness;
    private float liveness;
    private float valence;
    private float tempo;
    private int timeSignature;

    @Override
    public String toString() {
        return String.format("%s | Daily position: %d | %s (%s)", name, dailyRank, country, snapshotDate);
    }
}
