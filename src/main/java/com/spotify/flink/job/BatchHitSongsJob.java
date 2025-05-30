package com.spotify.flink.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spotify.flink.Main;
import com.spotify.flink.model.SongRecord;
import com.spotify.flink.model.SongRecordExtended;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

import static com.spotify.flink.processor.MarkHitSongs.*;
import static com.spotify.flink.processor.DataIngestionCleaner.loadAndCleanSongs;

public class BatchHitSongsJob {
    public static void run(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read CSV source (bounded)
        DataStream<SongRecord> songStream = loadAndCleanSongs(env, Main.DATASET_FILE);
        songStream.addSink(new DiscardingSink<>());

        DataStream<SongRecordExtended> hitSongs = songStream
                .keyBy(new SongKeySelector())
                .process(new HitSongDetector());

        DataStream<String> jsonStream = hitSongs.map(song -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            return mapper.writeValueAsString(song);
        });

        FileSink<String> fileSink = FileSink
                .forRowFormat(new Path(Main.OUTPUT_PATH), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(15))
                    .withInactivityInterval(Duration.ofMinutes(5))
                    .build())
                .build();

        jsonStream.sinkTo(fileSink);

//        hitSongs
//                .keyBy(SongRecordExtended::getCountry)
//                .process(new CountryHitSongMLProcessor())
//                .print();

        // Print the results
//        hitSongs.print();

//        DataStream<SongRecordExtended> roSongs = hitSongs
//                .filter(song -> song.getCountry().equals("RO"));
//
//        roSongs.print(); // This will print only RO records


        System.out.println("Starting Flink job...");
        JobExecutionResult result = env.execute("Spotify Hit Songs Detection");
        System.out.println("Flink job finished.");

        Long validCountLong = result.getAccumulatorResult(VALID_RECORDS);
        Long parsedCountLong = result.getAccumulatorResult(PARSED_RECORDS);
        Long hitSongsCountLong = result.getAccumulatorResult(HIT_SONGS_RECORDS);

        long validCount = (validCountLong != null) ? validCountLong : 0L;
        long parsedCount = (parsedCountLong != null) ? parsedCountLong : 0L;
        long hitSongsCount = (hitSongsCountLong != null) ? hitSongsCountLong : 0L;

        System.out.println("\n--- Final Aggregated Counts ---");
        System.out.println("Valid records (non-empty fields): " + validCount);
        System.out.println("Parsed records: " + parsedCount);
        System.out.println("Hit songs records: " + hitSongsCount);
        System.out.println("-------------------------------\n");
    }
}
