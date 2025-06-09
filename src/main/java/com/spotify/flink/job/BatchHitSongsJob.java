package com.spotify.flink.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spotify.flink.Main;
import com.spotify.flink.model.SongRecord;
import com.spotify.flink.model.SongRecordExtended;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

import static com.spotify.flink.processor.MarkHitSongs.*;
import static com.spotify.flink.processor.DataIngestionCleaner.loadAndCleanSongs;

public class BatchHitSongsJob {
    public static final MapStateDescriptor<String, SongRecordExtended> HIT_SONG_DESCRIPTOR =
            new MapStateDescriptor<>("hitSongsPerCountry", String.class, SongRecordExtended.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static void run() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read CSV source (bounded)
        DataStream<SongRecord> songStream = loadAndCleanSongs(env, Main.DATASET_FILE);
        songStream.addSink(new DiscardingSink<>());

        DataStream<SongRecordExtended> hitSongs = songStream
                .keyBy(new SongKeySelector())
                .process(new HitSongDetector());

        // Broadcast hit songs with key "country|spotifyId"
        BroadcastStream<SongRecordExtended> hitBroadcast = hitSongs.broadcast(HIT_SONG_DESCRIPTOR);

        // Connect and label all songs with hit=true/false
        DataStream<SongRecordExtended> labeledSongs = songStream
                .keyBy(new SongKeySelector())
                .connect(hitBroadcast)
                .process(new LabelHitSongs());

        // Write output as JSON
        DataStream<String> labeledJsonStream = labeledSongs.map(song -> mapper.writeValueAsString(song));

        FileSink<String> labeledFileSink = FileSink
                .forRowFormat(new Path(Main.OUTPUT_PATH), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(15))
                        .withInactivityInterval(Duration.ofMinutes(5))
                        .build())
                .build();

        labeledJsonStream.sinkTo(labeledFileSink);

        System.out.println("Starting Flink batch job: Spotify Hit Songs Detection...");
        JobExecutionResult result = env.execute("Spotify Hit Songs Detection");
        System.out.println("Flink batch job finished.");

        Long validCountLong = result.getAccumulatorResult(VALID_RECORDS);
        Long parsedCountLong = result.getAccumulatorResult(PARSED_RECORDS);
        Long hitSongsCountLong = result.getAccumulatorResult(HIT_SONGS_RECORDS);

        long validCount = (validCountLong != null) ? validCountLong : 0L;
        long parsedCount = (parsedCountLong != null) ? parsedCountLong : 0L;
        long hitSongsCount = (hitSongsCountLong != null) ? hitSongsCountLong : 0L;

        System.out.println("\n--- Final Aggregated Counts (batch job) ---");
        System.out.println("Valid records (non-empty fields): " + validCount);
        System.out.println("Parsed records: " + parsedCount);
        System.out.println("Total distinct hit songs detected: " + hitSongsCount);
        System.out.println("-------------------------------\n");
    }
}
