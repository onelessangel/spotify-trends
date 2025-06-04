package com.spotify.flink.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.spotify.flink.Main.*;

public class WorkingTreeHelper {
    public static void cleanWorkingTree() throws IOException {
        Path parentDir = Paths.get(OUTPUT_PATH);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(parentDir)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    deleteRecursively(path);
                }
            }
        }
    }

    public static void renameWorkingTree() throws IOException{
        Path parentDir = Paths.get(OUTPUT_PATH);

        try (Stream<Path> entries = Files.list(parentDir)) {
            List<Path> dirs = entries
                    .filter(Files::isDirectory)
                    .collect(Collectors.toList());

            if (dirs.size() == 1) {
                Path onlyDir = dirs.get(0);
                Path targetDir = parentDir.resolve(OUTPUT_DIR);
                // if targetDir exists, delete it first
                if (Files.exists(targetDir)) {
                    deleteRecursively(targetDir);
                }
                Files.move(onlyDir, targetDir, StandardCopyOption.ATOMIC_MOVE);
            }

            Path outputDir = parentDir.resolve(OUTPUT_DIR);
            if (Files.isDirectory(outputDir)) {
                try (Stream<Path> files = Files.list(outputDir)) {
                    List<Path> regularFiles = files
                            .filter(Files::isRegularFile)
                            .collect(Collectors.toList());

                    if (regularFiles.size() == 1) {
                        Path onlyFile = regularFiles.get(0);
                        String filename = onlyFile.getFileName().toString();
                        String ext = "";
                        int dot = filename.lastIndexOf('.');
                        if (dot >= 0) {
                            ext = filename.substring(dot);
                        }
                        Path targetFile = outputDir.resolve(HIT_SONGS_FILE + ext);
                        Files.move(onlyFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        }
    }

    private static void deleteRecursively(java.nio.file.Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc)
                    throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
