package com.spotify.flink.processor;

import com.spotify.flink.model.SongRecord;
import com.spotify.flink.model.SongRecordExtended;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MarkHitSongs {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final int TOP_RANK_THRESHOLD = 10;
    private static final int CONSECUTIVE_DAYS_THRESHOLD = 14;

    public static final String VALID_RECORDS = "validRecords";
    public static final String PARSED_RECORDS = "parsedRecords";
    public static final String HIT_SONGS_RECORDS = "hitSongsRecords";

    public static class SongKeySelector implements KeySelector<SongRecord, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(SongRecord record) {
            return new Tuple2<>(record.getCountry(), record.getSpotifyId());
        }
    }

    public static class HitSongDetector extends KeyedProcessFunction<Tuple2<String, String>, SongRecord, SongRecordExtended> {
        private ValueState<List<LocalDate>> topDaysState;
        private ValueState<Boolean> hasBeenReportedState;
        private transient LongCounter hitSongsRecords;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<List<LocalDate>> daysDescriptor =
                    new ValueStateDescriptor<>("top-rank-days", Types.LIST(Types.LOCAL_DATE));
            topDaysState = getRuntimeContext().getState(daysDescriptor);

            ValueStateDescriptor<Boolean> reportedDescriptor =
                    new ValueStateDescriptor<>("has-been-reported", Types.BOOLEAN);
            hasBeenReportedState = getRuntimeContext().getState(reportedDescriptor);

            hitSongsRecords = new LongCounter();
            getRuntimeContext().addAccumulator(HIT_SONGS_RECORDS, hitSongsRecords);
        }

        @Override
        public void processElement(
                SongRecord record,
                Context ctx,
                Collector<SongRecordExtended> out) throws Exception {

            List<LocalDate> topDays = topDaysState.value();
            if (topDays == null) {
                topDays = new ArrayList<>();
            }

            Boolean hasBeenReported = hasBeenReportedState.value();
            if (hasBeenReported == null) {
                hasBeenReported = false;
            }

            LocalDate currentDate = record.getSnapshotDate();

            if (record.getDailyRank() <= TOP_RANK_THRESHOLD) {
                // Only add if this date isn't already in the list
                if (!topDays.contains(currentDate)) {
                    topDays.add(currentDate);
                    topDays.sort(LocalDate::compareTo);
                }

                // Check for consecutive days
                int maxConsecutiveDays = 1;
                int currentConsecutiveDays = 1;
                LocalDate lastDate = null;

                for (LocalDate date : topDays) {
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
//                        topDays.stream()
//                            .map(d -> d.toString())
//                            .collect(java.util.stream.Collectors.joining(", ")));
//                }

                if (maxConsecutiveDays >= CONSECUTIVE_DAYS_THRESHOLD && !hasBeenReported) {
                    SongRecordExtended extendedRecord = new SongRecordExtended(record);
                    extendedRecord.setConsecutiveHitDaysCount(maxConsecutiveDays);
                    extendedRecord.setConsecutiveHitDaysList(topDays.stream().map(LocalDate::toString).collect(Collectors.toList()));

                    out.collect(extendedRecord);
                    hitSongsRecords.add(1);
                    hasBeenReported = true;
                    hasBeenReportedState.update(hasBeenReported);
                }
            } else {
                // Only remove this specific date if the song is not in top 25
                topDays.remove(currentDate);
            }

            topDaysState.update(topDays);
        }
    }
}
