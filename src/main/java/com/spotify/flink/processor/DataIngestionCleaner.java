package com.spotify.flink.processor;

import com.spotify.flink.Main;
import com.spotify.flink.model.SongRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.Objects;

import static com.spotify.flink.processor.MarkHitSongs.PARSED_RECORDS;
import static com.spotify.flink.processor.MarkHitSongs.VALID_RECORDS;

public class DataIngestionCleaner {
    public static DataStream<SongRecord> loadAndCleanSongs(final StreamExecutionEnvironment env, final String resourceName) {
        // Handle reading the file in a streaming and distributed style,
        // avoiding OutOfMemoryError for large files by not loading the entire content into driver memory
//        DataStream<String> rawData = env.readTextFile(resourceName); // songs.csv needs to be defined at the same level as pom.xml

        // Read from the classpath
        DataStream<String> rawData = env.addSource(new SourceFunction<>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        Objects.requireNonNull(Main.class.getClassLoader().getResourceAsStream(resourceName))
                ))) {
                    String line;
                    boolean isFirstLine = true;
                    while (isRunning && (line = reader.readLine()) != null) {
                        if (isFirstLine) {
                            isFirstLine = false;
                            continue; // Skip header
                        }
                        ctx.collect(line);
                    }
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // Parse each line of the CSV into SongRecord objects
        DataStream<SongRecord> parsed = rawData
                .flatMap(new RichFlatMapFunction<String, SongRecord>() {
                    // Counter (Flink's Metric system) - for proper distributed counting
                    private transient LongCounter validRecordsCounter;
                    private transient LongCounter parsedRecordsCounter;

                    // Initialize the counters
                    @Override
                    public void open(Configuration parameters) {
                        validRecordsCounter = new LongCounter();
                        parsedRecordsCounter = new LongCounter();

                        getRuntimeContext().addAccumulator(VALID_RECORDS, validRecordsCounter);
                        getRuntimeContext().addAccumulator(PARSED_RECORDS, parsedRecordsCounter);
                    }

                    @Override
                    public void flatMap(String line, Collector<SongRecord> out) {
                        try {
                            CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT.withQuote('"'));
                            for (CSVRecord record : parser) {
                                // Check if any field in the current record is null or blank
                                if (record.stream().anyMatch(s -> s == null || s.isBlank())) return;

                                validRecordsCounter.add(1);

                                // Parse the CSV record into a SongRecord object
                                SongRecord song = parseSong(record);
                                if (song != null) {
                                    parsedRecordsCounter.add(1);

                                    out.collect(song);
                                }
                            }
                        } catch (IOException e) {
                            System.err.println("Error parsing CSV line: '" + line + "' - " + e.getMessage());
                        } catch (Exception e) {
                            System.err.println("Unexpected error in flatMap for line: '" + line + "' - " + e.getMessage());
                        }
                    }
                })
                .returns(SongRecord.class);

        // Deduplicate records based on spotifyId using Flink's Keyed State
        return parsed;
    }

    private static SongRecord parseSong(CSVRecord record) {
        try {
            // Ensure the record has enough columns before attempting to access them
            if (record.size() < 25) {
                System.err.println("Record has too few columns. Expected 25, got " + record.size() + ". Record: " + record.toString());
                return null;
            }

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
