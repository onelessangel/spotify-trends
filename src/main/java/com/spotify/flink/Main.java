package com.spotify.flink;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Main {
    private static final AtomicInteger rawCount = new AtomicInteger();
    private static final AtomicInteger validCount = new AtomicInteger();
    private static final AtomicInteger parsedCount = new AtomicInteger();
    private static final AtomicInteger deduplicatedCount = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SongRecord> cleanedData = loadAndCleanSongs(env, "songs.csv");

        cleanedData.print();

        System.out.println("Starting Flink job...");
        env.execute("All Songs - Clean and Log Job");
        System.out.println("Flink job finished.");
    }

    private static DataStream<SongRecord> loadAndCleanSongs(StreamExecutionEnvironment env, String resourceName){
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(Main.class.getClassLoader().getResourceAsStream(resourceName))
        ));

        List<String> lines = reader.lines().collect(Collectors.toList());
        rawCount.set(lines.size());

        DataStream<String> rawData = env.fromCollection(lines);

        DataStream<SongRecord> parsed = rawData
                .flatMap(new RichFlatMapFunction<String, SongRecord>() {
                    @Override
                    public void flatMap(String line, Collector<SongRecord> out) {
                        try {
                            CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT.withQuote('"'));
                            for (CSVRecord record : parser) {
                                if (record.stream().anyMatch(s -> s == null || s.isBlank())) return;

                                validCount.incrementAndGet();

                                SongRecord song = parseSong(record);
                                if (song != null) {
                                    parsedCount.incrementAndGet();
                                    out.collect(song);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .returns(SongRecord.class);

        return parsed
                .keyBy(SongRecord::getSpotifyId)
                .filter(new RichFilterFunction<>() {
                    private transient ValueState<Boolean> seen;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Boolean.class, false);
                        seen = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public boolean filter(SongRecord value) throws Exception {
                        if (!seen.value()) {
                            seen.update(true);
                            int count = deduplicatedCount.incrementAndGet();
                            if (count % 100 == 0) {
                                System.out.println("Deduplicated count so far: " + count);
                            }
                            return true;
                        }
                        return false;
                    }
                })
                .map(song -> {
                    if (deduplicatedCount.get() == 1) {
                        System.out.println("Valid rows (non-empty): " + validCount.get());
                        System.out.println("Parsed records: " + parsedCount.get());
                    }
                    return song;
                });
    }

    private static SongRecord parseSong(CSVRecord record) {
        try {
            String snapshotDateStr = record.get(7).trim();
            if (snapshotDateStr.isEmpty()) return null;

            LocalDate snapshotDate = LocalDate.parse(snapshotDateStr);

            return new SongRecord(
                    record.get(0).trim(),                       // spotifyId
                    record.get(1).trim(),                       // name
                    Integer.parseInt(record.get(3).trim()),     // dailyRank
                    record.get(6).trim(),                       // country
                    snapshotDate,                               // snapshotDate
                    Boolean.parseBoolean(record.get(9).trim()), // isExplicit
                    Integer.parseInt(record.get(10).trim()),    // durationMs
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
        } catch (Exception e) {
            return null;
        }
    }
}
