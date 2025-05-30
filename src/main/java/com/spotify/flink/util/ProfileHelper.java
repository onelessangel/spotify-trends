package com.spotify.flink.util;

public class ProfileHelper {
    String describeSongProfile(double[] features) {
        StringBuilder sb = new StringBuilder();

        double explicit = features[0];
        double duration = features[1];
        double danceability = features[2];
        double energy = features[3];
        double key = features[4];
        double loudness = features[5];
        double mode = features[6];
        double speechiness = features[7];
        double acousticness = features[8];
        double instrumentalness = features[9];
        double liveness = features[10];
        double valence = features[11];
        double tempo = features[12];
        double timeSignature = features[13];

        sb.append(String.format("Explicit: %.2f → ~%.0f%% of songs marked as explicit.\n", explicit, explicit * 100));
        sb.append(String.format("Duration: %.1fs → Songs are ~%.1f minutes on average.\n", duration, duration / 60));
        sb.append(String.format("Danceability: %.2f → %s\n", danceability, describeScale(danceability, "Undanceable", "Highly danceable")));
        sb.append(String.format("Energy: %.2f → %s\n", energy, describeScale(energy, "Low energy", "High energy")));
        sb.append(String.format("Key: %.2f → Average key is %s.\n", key, keyToPitchClass((int)Math.round(key))));
        sb.append(String.format("Loudness: %.2f dB → %s\n", loudness, describeLoudness(loudness)));
        sb.append(String.format("Mode: %.2f → ~%.0f%% major (others minor).\n", mode, mode * 100));
        sb.append(String.format("Speechiness: %.3f → %s\n", speechiness, describeSpeechiness(speechiness)));
        sb.append(String.format("Acousticness: %.2f → %s\n", acousticness, describeScale(acousticness, "Mostly electronic", "Highly acoustic")));
        sb.append(String.format("Instrumentalness: %.3f → %s\n", instrumentalness, describeInstrumentalness(instrumentalness)));
        sb.append(String.format("Liveness: %.2f → %s\n", liveness, describeLiveness(liveness)));
        sb.append(String.format("Valence: %.2f → %s\n", valence, describeValence(valence)));
        sb.append(String.format("Tempo: %.1f BPM → %s\n", tempo, describeTempo(tempo)));
        sb.append(String.format("Time Signature: %.1f → Common time: %s\n", timeSignature, timeSignature == 4.0 ? "yes" : "no"));

        return sb.toString();
    }

    String describeScale(double value, String low, String high) {
        if (value < 0.3) return "Low - " + low;
        if (value > 0.7) return "High - " + high;
        return "Moderate";
    }

    String describeLoudness(double loudness) {
        if (loudness > -5) return "Very loud";
        if (loudness > -10) return "Moderately loud";
        return "Quiet";
    }

    String describeSpeechiness(double val) {
        if (val > 0.66) return "Likely speech or talk-show";
        if (val > 0.33) return "Somewhat speechy";
        return "Low spoken word (musical)";
    }

    String describeInstrumentalness(double val) {
        if (val > 0.7) return "Mostly instrumental";
        if (val > 0.3) return "Some instrumentals";
        return "Mostly vocal";
    }

    String describeLiveness(double val) {
        if (val > 0.8) return "Very likely live recording";
        if (val > 0.3) return "Some live ambiance";
        return "Studio recording";
    }

    String describeValence(double val) {
        if (val > 0.75) return "Very happy/positive";
        if (val > 0.4) return "Moderately positive";
        return "Sad or serious mood";
    }

    String describeTempo(double bpm) {
        if (bpm < 60) return "Very slow (ballad)";
        if (bpm < 90) return "Slow";
        if (bpm < 120) return "Moderate";
        if (bpm < 150) return "Fast";
        return "Very fast (intense)";
    }

    String keyToPitchClass(int key) {
        String[] keys = { "C", "C#/Db", "D", "D#/Eb", "E", "F", "F#/Gb", "G", "G#/Ab", "A", "A#/Bb", "B" };
        return keys[key % 12];
    }
}
