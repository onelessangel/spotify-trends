package com.spotify.flink;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
class SpotifySongRecord {
    private String spotifyId;
    private String name;
    private String artists;
    private int dailyRank;
    private String country;
    private LocalDate snapshotDate;
    private int popularity;
    private boolean isExplicit;
    private long durationMs;
    private String albumName;
    private LocalDate albumReleaseDate;
    private double danceability;
    private double energy;
    private int key;
    private double loudness;
    private int mode;
    private double speechiness;
    private double acousticness;
    private double instrumentalness;
    private double liveness;
    private double valence;
    private double tempo;
    private int timeSignature;
}

public class MarkHitSongs {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final int TOP_RANK_THRESHOLD = 10;
    private static final int CONSECUTIVE_DAYS_THRESHOLD = 14;

    private static class SongKeySelector implements KeySelector<SpotifySongRecord, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(SpotifySongRecord record) {
            return new Tuple2<>(record.getCountry(), record.getSpotifyId());
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read from the classpath
        DataStream<String> inputStream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        Objects.requireNonNull(MarkHitSongs.class.getClassLoader().getResourceAsStream("songs.csv"))
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

        // Parse CSV lines into SongRecord objects
        DataStream<SpotifySongRecord> songStream = inputStream
                .map((MapFunction<String, SpotifySongRecord>) line -> {
                    try (CSVParser parser = CSVParser.parse(new StringReader(line),
                            CSVFormat.DEFAULT.withQuote('"').withIgnoreSurroundingSpaces(true))) {
                        CSVRecord record = parser.getRecords().get(0);

                        SpotifySongRecord songRecord = new SpotifySongRecord();
                        songRecord.setSpotifyId(record.get(0).trim());
                        songRecord.setName(record.get(1).trim());
                        songRecord.setArtists(record.get(2).trim());
                        songRecord.setDailyRank(Integer.parseInt(record.get(3).trim()));
                        songRecord.setCountry(record.get(6).trim());

                        String snapshotDateStr = record.get(7).trim();
                        // Skip records with empty dates
                        if (snapshotDateStr.isEmpty()) {
                            return null; // Skip records with empty dates
                        }
                        // Skip records with invalid data
                        try {
                            songRecord.setSnapshotDate(LocalDate.parse(snapshotDateStr, DATE_FORMATTER));
                        } catch (DateTimeParseException e) {
                            return null;
                        }

                        songRecord.setPopularity(Integer.parseInt(record.get(8).trim()));
                        songRecord.setExplicit(Boolean.parseBoolean(record.get(9).trim()));
                        songRecord.setDurationMs(Long.parseLong(record.get(10).trim()));
                        songRecord.setAlbumName(record.get(11).trim());

                        // Handle album release date with validation
                        String albumDateStr = record.get(12).trim();
                        if (!albumDateStr.isEmpty()) {
                            // Set to null if invalid, but don't skip the record
                            try {
                                songRecord.setAlbumReleaseDate(LocalDate.parse(albumDateStr, DATE_FORMATTER));
                            } catch (DateTimeParseException e) {
                                songRecord.setAlbumReleaseDate(null);
                            }
                        }

                        songRecord.setDanceability(Double.parseDouble(record.get(13).trim()));
                        songRecord.setEnergy(Double.parseDouble(record.get(14).trim()));
                        songRecord.setKey(Integer.parseInt(record.get(15).trim()));
                        songRecord.setLoudness(Double.parseDouble(record.get(16).trim()));
                        songRecord.setMode(Integer.parseInt(record.get(17).trim()));
                        songRecord.setSpeechiness(Double.parseDouble(record.get(18).trim()));
                        songRecord.setAcousticness(Double.parseDouble(record.get(19).trim()));
                        songRecord.setInstrumentalness(Double.parseDouble(record.get(20).trim()));
                        songRecord.setLiveness(Double.parseDouble(record.get(21).trim()));
                        songRecord.setValence(Double.parseDouble(record.get(22).trim()));
                        songRecord.setTempo(Double.parseDouble(record.get(23).trim()));
                        songRecord.setTimeSignature(Integer.parseInt(record.get(24).trim()));

                        return songRecord;
                    }
                })
                .filter(Objects::nonNull); // Filter out records with invalid dates

        // Key by country and song ID, then process to track consecutive days in top
        DataStream<Tuple3<String, String, String>> hitSongs = songStream
//                .filter(song -> {
//                    return song.getCountry().equals("RO"); // uncomment to filter by country
//                })
                .keyBy(new SongKeySelector())
                .process(new HitSongDetector());

        // Print the results
        hitSongs.print();

        env.execute("Spotify Hit Songs Detection");
    }

    private static class HitSongDetector extends KeyedProcessFunction<Tuple2<String, String>, SpotifySongRecord, Tuple3<String, String, String>> {
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
                SpotifySongRecord record,
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
                        record.getName() + " by " + record.getArtists() + 
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
}
