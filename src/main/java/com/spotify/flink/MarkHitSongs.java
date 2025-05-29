package com.spotify.flink;

import lombok.Data;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


//@Data
//class SpotifySongRecord {
//    private String spotifyId;
//    private String name;
//    private String artists;
//    private int dailyRank;
//    private String country;
//    private LocalDate snapshotDate;
//    private int popularity;
//    private boolean isExplicit;
//    private long durationMs;
//    private String albumName;
//    private LocalDate albumReleaseDate;
//    private double danceability;
//    private double energy;
//    private int key;
//    private double loudness;
//    private int mode;
//    private double speechiness;
//    private double acousticness;
//    private double instrumentalness;
//    private double liveness;
//    private double valence;
//    private double tempo;
//    private int timeSignature;
//}

public class MarkHitSongs {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final int TOP_RANK_THRESHOLD = 10;
    private static final int CONSECUTIVE_DAYS_THRESHOLD = 14;
    private static final String VALID_RECORDS = "validRecords";
    private static final String PARSED_RECORDS = "parsedRecords";
    private static final String DEDUPLICATED_RECORDS = "deduplicatedRecords";

    private static class SongKeySelector implements KeySelector<SongRecord, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(SongRecord record) {
            return new Tuple2<>(record.getCountry(), record.getSpotifyId());
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read from the classpath
        DataStream<SongRecord> songStream = loadAndCleanSongs(env, "songs.csv");
        songStream.addSink(new DiscardingSink<>());

        // Parse CSV lines into SongRecord objects

        // Key by country and song ID, then process to track consecutive days in top
//        DataStream<Tuple3<String, String, String>> hitSongs = songStream
//                .filter(song -> {
//                    return song.getCountry().equals("RO"); // uncomment to filter by country
//                })
//                .keyBy(new SongKeySelector())
//                .process(new HitSongDetector());

        DataStream<Tuple3<String, String, String>> hitSongs = songStream
//                .filter(song -> song.getDailyRank() <= 10)
                .keyBy(new SongKeySelector())
                .process(new HitSongDetector());

        // Print the results
        hitSongs.print();

        env.execute("Spotify Hit Songs Detection");
    }

    private static class HitSongDetector extends KeyedProcessFunction<Tuple2<String, String>, SongRecord, Tuple3<String, String, String>> {
        private ValueState<List<LocalDate>> topDaysState;
        private ValueState<Boolean> hasBeenReportedState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<List<LocalDate>> daysDescriptor =
                    new ValueStateDescriptor<>("top25-days", Types.LIST(Types.LOCAL_DATE));
            topDaysState = getRuntimeContext().getState(daysDescriptor);

            ValueStateDescriptor<Boolean> reportedDescriptor =
                    new ValueStateDescriptor<>("has-been-reported", Types.BOOLEAN);
            hasBeenReportedState = getRuntimeContext().getState(reportedDescriptor);
        }

        @Override
        public void processElement(
                SongRecord record,
                Context ctx,
                Collector<Tuple3<String, String, String>> out) throws Exception {

            List<LocalDate> top25Days = topDaysState.value();
            if (top25Days == null) {
                top25Days = new ArrayList<>();
            }

            Boolean hasBeenReported = hasBeenReportedState.value();
            if (hasBeenReported == null) {
                hasBeenReported = false;
            }

            LocalDate currentDate = record.getSnapshotDate();

            if (record.getDailyRank() <= TOP_RANK_THRESHOLD) {
                // Only add if this date isn't already in the list
                if (!top25Days.contains(currentDate)) {
                    top25Days.add(currentDate);
                    top25Days.sort(LocalDate::compareTo);
                }

                // Check for consecutive days
                int maxConsecutiveDays = 1;
                int currentConsecutiveDays = 1;
                LocalDate lastDate = null;

                for (LocalDate date : top25Days) {
                    if (lastDate != null) {
                        long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(lastDate, date);
                        if (daysBetween == 1) {
                            currentConsecutiveDays++;
                            maxConsecutiveDays = Math.max(maxConsecutiveDays, currentConsecutiveDays);
                        } else if (daysBetween > 1) {
                            // If gap is more than 1 day, reset the consecutive count
                            currentConsecutiveDays = 1;
                        }
                    }

                    lastDate = date;
                }

                // For debugging: uncomment to print songs that were close to be marked as hits
//                if (maxConsecutiveDays >= 10 && !hasBeenReported) {
//                    System.out.println("Song " + record.getName() + " by " + record.getArtists() +
//                        " has " + maxConsecutiveDays + " consecutive days in top 25. Dates: " +
//                        top25Days.stream()
//                            .map(d -> d.toString())
//                            .collect(java.util.stream.Collectors.joining(", ")));
//                }

                if (maxConsecutiveDays >= CONSECUTIVE_DAYS_THRESHOLD && !hasBeenReported) {
                    out.collect(new Tuple3<>(
                            record.getCountry(),
                            record.getSpotifyId(),
                            record.getName() +
                                    " (" + maxConsecutiveDays + " days: " +
                                    top25Days.stream()
                                            .map(LocalDate::toString)
                                            .collect(java.util.stream.Collectors.joining(", ")) + ")"
                    ));
                    hasBeenReported = true;
                    hasBeenReportedState.update(hasBeenReported);
                }
            } else {
                // Only remove this specific date if the song is not in top 25
                top25Days.remove(currentDate);
            }

            topDaysState.update(top25Days);
        }
    }
    private static DataStream<SongRecord> loadAndCleanSongs(final StreamExecutionEnvironment env, final String resourceName) {
        // Handle reading the file in a streaming and distributed style,
        // avoiding OutOfMemoryError for large files by not loading the entire content into driver memory
//        DataStream<String> rawData = env.readTextFile(resourceName); // songs.csv needs to be defined at the same level as pom.xml

        // Read from the classpath
        DataStream<String> rawData = env.addSource(new SourceFunction<String>() {
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
        return parsed
                ;
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