package com.spotify.flink.util;

import com.spotify.flink.model.SongRecordExtended;
import smile.data.vector.IntVector;

/**
 * Util class for extracting the numerical features for the song. These will be used
 * both for K-Means clustering and Random Forest classification.
 */
public class FeatureExtractor {
    /**
     * Extract the numerical features for the song.
     *
     * @param song SongRecordExtended
     * @return numerical features
     */
    public static double[] extractFeatures(SongRecordExtended song) {
        return new double[]{
                song.isExplicit() ? 1.0 : 0.0,
                song.getDurationMs() / 1000.0, // converting ms to s
                song.getDanceability(),
                song.getEnergy(),
                song.getKey(),
                song.getLoudness(),
                song.getMode(),
                song.getSpeechiness(),
                song.getAcousticness(),
                song.getInstrumentalness(),
                song.getLiveness(),
                song.getValence(),
                song.getTempo(),
                song.getTimeSignature()
        };
    }
}
