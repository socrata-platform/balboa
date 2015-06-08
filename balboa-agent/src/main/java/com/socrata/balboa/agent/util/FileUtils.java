package com.socrata.balboa.agent.util;


import java.io.File;
import java.io.FileFilter;

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


}
