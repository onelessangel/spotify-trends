package com.spotify.flink.job;

import com.spotify.flink.Main;
import com.spotify.flink.model.SongRecordExtended;
import com.spotify.flink.processor.CountryHitSongMLProcessor;
import com.spotify.flink.util.JsonToSongRecordExtendedMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingMLJob {
    public static void run(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(Main.OUTPUT_PATH + "/" + Main.OUTPUT_DIR))
                .monitorContinuously(Time.seconds(30).toDuration())
                .build();

        DataStream<String> jsonLines = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                Main.HIT_SONGS_FILE);

        DataStream<SongRecordExtended> processedHitSongs = jsonLines
                .map(new JsonToSongRecordExtendedMapper())
                .returns(SongRecordExtended.class);

        DataStream<String> profilesAndPredictions = processedHitSongs
                .keyBy(SongRecordExtended::getCountry)
                .process(new CountryHitSongMLProcessor());


        profilesAndPredictions.print();

        System.out.println("Starting Flink streaming job: Spotify ML Processing...");
        env.execute("Streaming Hit Song ML Processing");
    }
}
