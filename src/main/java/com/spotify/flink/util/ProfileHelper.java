package com.spotify.flink.util;

public class ProfileHelper {
    public String describeSongProfile(double[] features) {
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

        sb.append(String.format("Explicit: ~%.0f%% of song marked explicit\n", explicit * 100));
        sb.append(String.format("Duration: %.1fs (~%.1f min)\n", duration, duration / 60));
        sb.append(String.format("Danceability: %.2f (%s)\n", danceability, describeScale(danceability, "undanceable", "highly danceable")));
        sb.append(String.format("Energy: %.2f (%s).\n", energy, describeScale(energy, "low energy", "high energy")));
        sb.append(String.format("Key: %s\n", keyToPitchClass((int) Math.round(key))));
        sb.append(String.format("Loudness: %.2f dB (%s)\n", loudness, describeLoudness(loudness)));
        sb.append(String.format("Mode: ~%.0f%% major\n", mode * 100));
        sb.append(String.format("Speechiness: %.3f (%s)\n", speechiness, describeSpeechiness(speechiness)));
        sb.append(String.format("Acousticness: %.2f (%s)\n", acousticness, describeScale(acousticness, "electronic", "acoustic")));
        sb.append(String.format("Instrumentalness: %.3f (%s)\n", instrumentalness, describeInstrumentalness(instrumentalness)));
        sb.append(String.format("Liveness: %.2f (%s)\n", liveness, describeLiveness(liveness)));
        sb.append(String.format("Valence: %.2f (%s mood)\n", valence, describeValence(valence)));
        sb.append(String.format("Tempo: %.1f BPM (%s)\n", tempo, describeTempo(tempo)));
        sb.append(String.format("Time Signature: %.1f (%s)\n", timeSignature, timeSignature == 4.0 ? "common time" : "unusual meter"));

        return sb.toString();
    }

    private String describeScale(double value, String low, String high) {
        if (value < 0.3) return low;
        if (value > 0.7) return high;
        return "moderate";
    }

    private String describeLoudness(double loudness) {
        if (loudness > -5) return "very loud";
        if (loudness > -10) return "moderately loud";
        return "quiet";
    }

    private String describeSpeechiness(double val) {
        if (val > 0.66) return "talk-like (e.g., podcast)";
        if (val > 0.33) return "some speech";
        return "musical";
    }

    private String describeInstrumentalness(double val) {
        if (val > 0.7) return "mostly instrumental";
        if (val > 0.3) return "partly instrumental";
        return "mostly vocal";
    }

    private String describeLiveness(double val) {
        if (val > 0.8) return "live performance";
        if (val > 0.3) return "live-like ambiance";
        return "studio recording";
    }

    private String describeValence(double val) {
        if (val > 0.75) return "happy";
        if (val > 0.4) return "positive";
        return "serious";
    }

    private String describeTempo(double bpm) {
        if (bpm < 60) return "very slow";
        if (bpm < 90) return "slow";
        if (bpm < 120) return "moderate";
        if (bpm < 150) return "fast";
        return "very fast";
    }

    private String keyToPitchClass(int key) {
        String[] keys = {"C", "C#/Db", "D", "D#/Eb", "E", "F", "F#/Gb", "G", "G#/Ab", "A", "A#/Bb", "B"};
        return keys[key % 12];
    }

    public String shortSummary(double[] features) {
        if (features == null || features.length < 14) return "unknown song";

        double valence = features[11];
        double energy = features[3];
        double tempo = features[12];
        double danceability = features[2];
        double acousticness = features[8];

        String moodWord = moodWord(valence);
        String tempoWord = tempoWord(tempo, energy);
        String danceWord = danceWord(danceability);
        String genreWord = genreWord(acousticness, energy);

        String description = moodWord;

        if (!tempoWord.isEmpty()) {
            if (!description.isEmpty()) description += " ";
            description += tempoWord;
        }
        if (!danceWord.isEmpty()) {
            if (!description.isEmpty()) description += " ";
            description += danceWord;
        }
        if (!genreWord.isEmpty()) {
            if (!description.isEmpty()) description += " ";
            description += genreWord;
        } else {
            if (!description.isEmpty()) description += " ";
            description += "song";
        }

        return description.trim();
    }

    private String moodWord(double valence) {
        if (valence > 0.75) return "upbeat";
        if (valence > 0.5) return "bright";
        if (valence > 0.3) return "moody";
        return "dark";
    }

    private String tempoWord(double tempo, double energy) {
        if (tempo < 70) return "slow";
        if (tempo < 110) return energy > 0.6 ? "steady" : "calm";
        if (tempo < 140) return "energetic";
        return "fast";
    }

    private String danceWord(double danceability) {
        if (danceability > 0.7) return "danceable";
        if (danceability > 0.4) return "groovy";
        return "";
    }

    private String genreWord(double acousticness, double energy) {
        if (acousticness > 0.7) {
            if (energy < 0.4) return "jazz song";
            else return "folk song";
        } else {
            if (energy > 0.7) return "pop song";
            if (energy > 0.4) return "rock song";
            return "electronic song";
        }
    }
}
