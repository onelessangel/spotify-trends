package com.spotify.flink.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spotify.flink.model.SongRecordExtended;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToSongRecordExtendedMapper implements MapFunction<String, SongRecordExtended> {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public SongRecordExtended map(String line) throws Exception {
        return mapper.readValue(line, SongRecordExtended.class);
    }
}
