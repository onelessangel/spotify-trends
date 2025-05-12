package com.spotify.flink;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(Main.class.getClassLoader().getResourceAsStream("songs.csv"))
        ));

        List<String> lines = reader.lines().collect(Collectors.toList());
        DataStream<String> rawData = env.fromCollection(lines);

        DataStream<SongRecord> romaniaTop10 = rawData
                .flatMap((String line, Collector<SongRecord> out) -> {
                    try {
                        CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT.withQuote('"'));

                        for (CSVRecord record : parser) {
                            int dailyRank = Integer.parseInt(record.get(3).trim());
                            String country = record.get(6).trim();
                            String snapshotDate = record.get(7).trim();

                            if (country.equalsIgnoreCase("RO") && dailyRank <= 10 && snapshotDate.equals("2025-05-09")) {
                                SongRecord song = parseSong(record);
                                out.collect(song);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).returns(SongRecord.class);

        romaniaTop10.print();

        System.out.println("Starting Flink job...");
        env.execute("Top 10 Romania Songs - Flink Job");
        System.out.println("Flink job finished.");
    }

    private static SongRecord parseSong(CSVRecord record) {
        return new SongRecord(
                record.get(0).trim(),                       // spotifyId
                record.get(1).trim(),                       // name
                record.get(2).trim(),                       // artists
                Integer.parseInt(record.get(3).trim()),     // dailyRank
                Integer.parseInt(record.get(4).trim()),     // dailyMovement
                Integer.parseInt(record.get(5).trim()),     // weeklyMovement
                record.get(6).trim(),                       // country
                record.get(7).trim(),                       // snapshotDate
                Integer.parseInt(record.get(8).trim()),     // popularity
                Boolean.parseBoolean(record.get(9).trim()), // isExplicit
                Integer.parseInt(record.get(10).trim()),    // durationMs
                record.get(11).trim(),                      // albumName
                record.get(12).trim(),                      // albumReleaseDate
                Float.parseFloat(record.get(13).trim()),    // danceability
                Float.parseFloat(record.get(14).trim()),    // energy
                Integer.parseInt(record.get(15).trim()),    // key
                Float.parseFloat(record.get(16).trim()),    // loudness
                Integer.parseInt(record.get(17).trim()),    // mode
                Float.parseFloat(record.get(18).trim()),    // speechiness
                Float.parseFloat(record.get(19).trim()),    // acousticness
                Float.parseFloat(record.get(20).trim()),    // instrumentalness
                Float.parseFloat(record.get(21).trim()),    // liveness
                Float.parseFloat(record.get(22).trim()),    // valence
                Float.parseFloat(record.get(23).trim()),    // tempo
                Integer.parseInt(record.get(24).trim())     // timeSignature
        );
    }
}
