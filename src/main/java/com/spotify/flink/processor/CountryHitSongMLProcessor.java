package com.spotify.flink.processor;

import com.spotify.flink.model.SongRecordExtended;
import com.spotify.flink.util.ProfileHelper;
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
            long timerTs = now + 30_000; // 30 seconds later;
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
        KMeans model = KMeans.fit(featureMatrix, 4);

        // song profiler
        ProfileHelper profileHelper = new ProfileHelper();

        // Build a profile string reporting cluster centers
        StringBuilder profile = new StringBuilder();
        profile.append("Country: ").append(context.getCurrentKey()).append("\n");
        profile.append("Cluster centroids:\n");

        int clusterIndex = 1;
        for (double[] centroid : model.centroids) {
            profile.append("Cluster ").append(clusterIndex++).append(" Summary:\n");
            profile.append("→ ").append(profileHelper.shortSummary(centroid)).append("\n");
            profile.append(profileHelper.describeSongProfile(centroid)).append("\n");
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
