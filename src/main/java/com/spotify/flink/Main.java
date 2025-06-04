package com.spotify.flink;

import com.spotify.flink.job.BatchHitSongsJob;
import com.spotify.flink.job.StreamingMLJob;
import com.spotify.flink.util.WorkingTreeHelper;

public class Main {
    public static final String DATASET_FILE = "songs.csv";
    public static final String OUTPUT_PATH = "src/main/resources";
    public static final String OUTPUT_DIR = "output";
    public static final String HIT_SONGS_FILE = "hit_songs";

    public static void main(String[] args) throws Exception {
        WorkingTreeHelper.cleanWorkingTree();
        BatchHitSongsJob.run(args);
        WorkingTreeHelper.renameWorkingTree();
        StreamingMLJob.run(args);
    }
}
