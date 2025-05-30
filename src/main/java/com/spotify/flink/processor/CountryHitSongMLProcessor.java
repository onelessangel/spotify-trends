package com.spotify.flink.processor;

import com.spotify.flink.model.SongRecordExtended;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import smile.clustering.KMeans;

import java.util.ArrayList;
import java.util.List;

public class CountryHitSongMLProcessor extends KeyedProcessFunction<String, SongRecordExtended, String> {
    private transient ListState<SongRecordExtended> buffer;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<SongRecordExtended> descriptor =
                new ListStateDescriptor<>("countryHitSongs", TypeInformation.of(new TypeHint<>() {}));
        buffer = getRuntimeContext().getListState(descriptor);

        // initialize timerState to keep track of the registered timer
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timerState", Long.class);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            SongRecordExtended song,
            KeyedProcessFunction<String, SongRecordExtended, String>.Context context,
            Collector<String> out) throws Exception {
        buffer.add(song);

        // register timer only if not already registered
        Long currentTimer = timerState.value();
        long now = context.timerService().currentProcessingTime();
        if (currentTimer == null || currentTimer <= now) {
            long timerTs = now + 60_000; // 1 minute later;
            context.timerService().registerProcessingTimeTimer(timerTs);
            timerState.update(timerTs);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<String, SongRecordExtended, String>.OnTimerContext context,
            Collector<String> out) throws Exception {
        List<SongRecordExtended> songs = new ArrayList<>();
//        System.out.println("Timer fired at " + timestamp + " for key " + context.getCurrentKey());

        for (SongRecordExtended record : buffer.get()) {
            songs.add(record);
        }

        buffer.clear();

        // clear timer state so next elements can schedule new timers
        timerState.clear();

        if (songs.isEmpty() || songs.size() < 2) {
            // Not enough data to train
            return;
        }

        List<double[]> featuresList = new ArrayList<>();
        for (SongRecordExtended song : songs) {
            featuresList.add(extractFeatures(song));
        }

        double[][] featureMatrix = featuresList.toArray(new double[0][]);

        // Train KMeans model (e.g., 2 clusters)
        KMeans model = KMeans.fit(featureMatrix, 2);

        String[] featureNames = new String[] {
                "Explicit (1 = yes, 0 = no)",
                "Duration (seconds)",
                "Danceability (0.0–1.0)",
                "Energy (0.0–1.0)",
                "Key (Pitch Class: 0 = C, 1 = C#/Db, ..., 11 = B)",
                "Loudness (dB)",
                "Mode (1 = major, 0 = minor)",
                "Speechiness (0.0–1.0)",
                "Acousticness (0.0–1.0)",
                "Instrumentalness (0.0–1.0)",
                "Liveness (0.0–1.0)",
                "Valence (0.0–1.0, positivity)",
                "Tempo (BPM)",
                "Time Signature (beats per bar)"
        };

        // Build a profile string reporting cluster centers
        StringBuilder profile = new StringBuilder();
        profile.append("Country: ").append(context.getCurrentKey()).append("\n");
        profile.append("Cluster centroids:\n");

        int clusterIndex = 1;
        for (double[] centroid : model.centroids) {
            profile.append("Centroid ").append(clusterIndex++).append(":\n");
            for (int i = 0; i < centroid.length; i++) {
                profile.append("  ").append(featureNames[i]).append(": ").append(centroid[i]).append("\n");
            }
            profile.append("\n");
        }

        out.collect(profile.toString());
    }


    private double[] extractFeatures(SongRecordExtended song) {
        return new double[] {
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
