package com.practice.kafka.event;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileEventSource implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FileEventSource.class);
    private File file;
    private long filePointer = 0;

    private boolean keepRunning = true;
    private int updateInterval;

    private EventHandler eventHandler;

    public FileEventSource(File file, int updateInterval, EventHandler eventHandler) {
        this.file = file;
        this.updateInterval = updateInterval;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);
                // file의 크기를 계산.
                long length = this.file.length();
                if (length < this.filePointer) {
                    log.info("file was reset as filePointer is longer than file length");
                    filePointer = length;
                } else if(length > this.filePointer) {
                    readAppendAndSend();
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void readAppendAndSend() throws ExecutionException, IOException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
    }
}
