package com.spotify.flink;

import com.spotify.flink.model.SongRecordExtended;
import com.spotify.flink.util.FeatureExtractor;
import smile.classification.RandomForest;
import smile.data.DataFrame;
import smile.data.vector.IntVector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;

import java.util.stream.IntStream;

public class RandomForestModelTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // Example country model to load
        String country = "RO";
        String modelPath = Paths.get(Main.OUTPUT_PATH, "models", country + ".model").toString();

        // Load the RandomForest model from file
        RandomForest rf;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(modelPath))) {
            rf = (RandomForest) ois.readObject();
        }

        System.out.println("Loaded RandomForest model for country: " + country);

        // Create a sample SongRecordExtended for prediction
        SongRecordExtended sampleSong = createSampleSong();

        // Prepare features for the sample song
        double[] features = FeatureExtractor.extractFeatures(sampleSong);
        int numFeatures = features.length;

        // Feature names f1, f2, ..., fN (should match training)
        String[] featureNames = IntStream.range(0, numFeatures)
                .mapToObj(i -> "f" + (i + 1))
                .toArray(String[]::new);

        // Create DataFrame with features + dummy label (required by Smile)
        DataFrame predictDf = DataFrame.of(new double[][]{features}, featureNames)
                .merge(IntVector.of("label", new int[]{0}));

        // Predict class label
        int prediction = rf.predict(predictDf)[0];

        System.out.println("\nSample Song: " + sampleSong.getName());
        System.out.println("Predicted class: " + (prediction == 1 ? "HIT" : "NOT HIT"));
    }

    private static SongRecordExtended createSampleSong() {
        // TODO: Customize this with realistic feature values to test
        SongRecordExtended song = new SongRecordExtended();
        song.setName("Sample Song");
        song.setExplicit(false);
        song.setDurationMs(210000);  // 3.5 minutes
        song.setDanceability(0.8);
        song.setEnergy(0.75);
        song.setKey(5);
        song.setLoudness(-5.0);
        song.setMode(1);
        song.setSpeechiness(0.05);
        song.setAcousticness(0.1);
        song.setInstrumentalness(0.0);
        song.setLiveness(0.12);
        song.setValence(0.65);
        song.setTempo(120.0);
        song.setTimeSignature(4);
        return song;
    }
}
