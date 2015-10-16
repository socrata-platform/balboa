package com.socrata.balboa.agent.util;


import java.io.File;
import java.io.FileFilter;
import java.util.HashSet;
import java.util.Set;

public class FileUtils {

    public static final String BROKEN_FILE_EXTENSION = ".broken";
    public static final String LOCK_FILE_EXTENSION = ".lock";

    public static final FileFilter isFile = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.isFile() && !pathname.getName().endsWith(BROKEN_FILE_EXTENSION) && !pathname.getName().endsWith(LOCK_FILE_EXTENSION);
        }
    };

    public static final FileFilter isDirectory = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };

    /**
     * Recursively extracts all the directories nested under a parent directory.
     *
     * @param directory The root directory to recursively search.
     * @return The set of directories including the argument directory.
     */
    public static Set<File> getDirectories(File directory) {
        Set<File> directories = new HashSet<>();
        if (!directory.isDirectory()) { // Return quickly if there
            return directories;
        }
        directories.add(directory);
        for (File child: directory.listFiles(FileUtils.isDirectory)) {
            directories.addAll(getDirectories(child));
        }
        return directories;
    }
}
