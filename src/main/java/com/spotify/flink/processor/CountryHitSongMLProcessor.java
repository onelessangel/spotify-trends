package com.spotify.flink.processor;

import com.spotify.flink.Main;
import com.spotify.flink.model.SongRecordExtended;
import com.spotify.flink.util.ProfileHelper;
import com.spotify.flink.util.FeatureExtractor;
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
import smile.classification.RandomForest;
import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.data.vector.IntVector;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CountryHitSongMLProcessor extends KeyedProcessFunction<String, SongRecordExtended, String> {
    private transient ListState<SongRecordExtended> buffer;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<SongRecordExtended> descriptor =
                new ListStateDescriptor<>("countryHitSongs", TypeInformation.of(new TypeHint<>() {
                }));
        buffer = getRuntimeContext().getListState(descriptor);

        // Initialize timerState to keep track of the registered timer
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timerState", Long.class);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    private static void saveModel(RandomForest model, String country) throws IOException {
        String outputFolder = Main.OUTPUT_PATH + "/models";
        // Build the full path safely
        Path path = Paths.get(outputFolder);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Path filePath = Paths.get(outputFolder, country + ".model");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath.toFile()))) {
            oos.writeObject(model);
            System.out.println("Model for " + country + " saved as " + filePath + "\n");
        }
    }

    @Override
    public void processElement(
            SongRecordExtended song,
            KeyedProcessFunction<String, SongRecordExtended, String>.Context context,
            Collector<String> out) throws Exception {
        buffer.add(song);

        // Register timer only if not already registered
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

        // Clear buffer for the next period
        buffer.clear();

        // Clear timer state so next elements can schedule new timers
        timerState.clear();

        if (songs.isEmpty() || songs.size() < 2) {
            // Not enough data to train
            return;
        }

        List<double[]> featuresList = new ArrayList<>();
        List<Integer> labelsList = new ArrayList<>();

        // For KMeans, only train on hit songs
        List<SongRecordExtended> hitSongs = songs.stream()
                .filter(SongRecordExtended::isHit)
                .collect(Collectors.toList());

        // Extract features for hit songs only (for clustering)
        double[][] hitFeatures = hitSongs.stream()
                .map(FeatureExtractor::extractFeatures)
                .toArray(double[][]::new);

        StringBuilder modelsOutput = new StringBuilder()
                .append("Country: ").append(context.getCurrentKey())
                .append("\nRecords processed: ").append(songs.size())
                .append("\n--- ML Models Output ---\n");

        // KMeans
        if (hitFeatures.length > 0) {
            applyKMeans(hitFeatures, modelsOutput);
        } else {
            modelsOutput.append("\nNo hit songs available for KMeans clustering.\n");
        }

        // Random Forest
        // Choose a song for prediction
        SongRecordExtended sampleSongForPrediction = hitSongs.get(0);

        // Collect "isHit" label for Random Forest
        for (SongRecordExtended song : songs) {
            featuresList.add(FeatureExtractor.extractFeatures(song));
            labelsList.add(song.isHit() ? 1 : 0);
        }

        double[][] featureMatrix = featuresList.toArray(new double[0][]);
        int[] labels = labelsList.stream().mapToInt(i -> i).toArray();

        // Only run RF if we have both classes
        long hitCount = labelsList.stream().filter(l -> l == 1).count();
        long nonHitCount = labelsList.size() - hitCount;

        if (hitCount == 0 || nonHitCount == 0) {
            modelsOutput.append("\nSkipping RF training: need both hits and non-hits. Hits: ")
                    .append(hitCount).append(", Non-hits: ").append(nonHitCount).append("\n");
        } else {
            applyRandomForest(featureMatrix, labels, sampleSongForPrediction, modelsOutput, context.getCurrentKey());
        }

        out.collect(modelsOutput.toString());
    }

    private void applyKMeans(double[][] featureMatrix, StringBuilder profile) {
        try {
            // Train KMeans model (e.g., 4 clusters)
            KMeans modelKMeans = KMeans.fit(featureMatrix, 4);

            // Song profiler
            ProfileHelper helper = new ProfileHelper();

            profile.append("\nK-Means Cluster Centroids:\n");

            int clusterIndex = 1;
            for (double[] centroid : modelKMeans.centroids) {
                profile.append("Cluster ").append(clusterIndex++).append(":\n→ ")
                        .append(helper.shortSummary(centroid)).append("\n")
                        .append(helper.describeSongProfile(centroid)).append("\n");
            }
        } catch (Exception e) {
            profile.append("\nK-Means FAILED");
        }
    }

    private void applyRandomForest(double[][] features, int[] labels, SongRecordExtended song, StringBuilder modelsOutput, String country) {
        try {
            int numFeatures = features[0].length;

            String[] featureNames = IntStream.range(0, numFeatures)
                    .mapToObj(i -> "f" + (i + 1))
                    .toArray(String[]::new);

            DataFrame df = DataFrame.of(features, featureNames).merge(IntVector.of("label", labels));
            RandomForest rf = RandomForest.fit(Formula.lhs("label"), df);

            modelsOutput.append("\n--- Random Forest Classification ---\n");
            modelsOutput.append("Trained RF with ").append(numFeatures).append(" features.\n");

            // Test prediction
            double[][] predictFeatures = new double[][]{FeatureExtractor.extractFeatures(song)};
            DataFrame predictDf = DataFrame.of(predictFeatures, featureNames)
                    .merge(IntVector.of("label", new int[]{0}));

            int prediction = rf.predict(predictDf)[0];

            modelsOutput.append("\n--- Prediction Random Forest ---\nSample song: '")
                    .append(song.getName()).append("' is predicted to be ")
                    .append(prediction == 1 ? "a HIT." : "not a HIT.").append("\n");

            // Save model
            saveModel(rf, country);
        } catch (Exception e) {
            modelsOutput.append("\nRandom Forest FAILED\n");
            modelsOutput.append(e.getMessage());
        }
    }
}
