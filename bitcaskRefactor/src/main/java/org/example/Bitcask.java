package org.example;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Bitcask {
    public static final String EXTENSION = ".BitCask";
    public static final String HINT_EXTENSION = ".Hint.BitCask";
    public static final String COMPACT_NAME = "compact";
    private static final int MAX_FILE_SIZE = 1024 * 2; // 1GB
    private static final int MAX_SEQ = 5;
    private final ConcurrentHashMap<Integer, FilePointer> logIndex;
    private final FileOperations fileOperations;
    private final Object putLock = new Object();
    private final Object getLock = new Object();
    private final AtomicBoolean isCompacting = new AtomicBoolean(false);
    private AtomicLong currentSeq;
    private File activeFile;
    private String parentPath;
    private RandomAccessFile rafActiveFile;
    private long dirCreationTime;

    public Bitcask() {
        currentSeq = new AtomicLong(0);
        this.logIndex = new ConcurrentHashMap<>();
        this.fileOperations = new FileOperations();
    }


    private void recover(File[] files) throws IOException {
        HashSet<Integer> fileIds = new HashSet<>();
        fileOperations.recoverHintFiles(files, fileIds, logIndex);
        fileOperations.recoverDataFiles(files, fileIds, logIndex);
        System.out.println("Recovered");
    }

    public void open(String path) throws IOException {
        this.parentPath = path;
        this.fileOperations.createDirectory(path);
        File[] files = this.fileOperations.listFiles(path);
        currentSeq = new AtomicLong(fileOperations.getMaxSeq(files));
        this.activeFile = new File(path + File.separator + "0" + EXTENSION);
        if (currentSeq.get() == 0 && !this.activeFile.exists()) {
            boolean created = this.activeFile.createNewFile();
            if (!created) {
                System.err.println("Failed to create active file");
            }
        }

        this.rafActiveFile = new RandomAccessFile(this.activeFile, "rw");
        Path p = Path.of(path);
        try {
            BasicFileAttributes attr = Files.readAttributes(p, BasicFileAttributes.class);
            dirCreationTime = attr.creationTime().toMillis();
            recover(files);
            System.out.println("Opened");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void put(int key, String value) throws IOException {
        synchronized (putLock) {
            byte[] valueBytes = value.getBytes();
            int valueSize = valueBytes.length;
            int keySize = Integer.BYTES;
            long timestamp = System.currentTimeMillis() - dirCreationTime;
            long valueOffset = rafActiveFile.getFilePointer();
            BitCaskFileEntry entry = new BitCaskFileEntry(timestamp, keySize, valueSize, key, valueBytes);
            FilePointer filePointer = new FilePointer(currentSeq.get(), valueBytes.length, valueOffset, timestamp);
            logIndex.put(key, filePointer);
            fileOperations.writeDataEntry(rafActiveFile, entry);

            // Debug statement to verify data is written
            System.out.println("Written to active file: Key=" + key + ", Value=" + value);

            if (rafActiveFile.length() > MAX_FILE_SIZE) {
                currentSeq.incrementAndGet();
                activeFile = new File(parentPath + File.separator + currentSeq + EXTENSION);
                boolean created = activeFile.createNewFile();
                if (!created) {
                    System.err.println("Failed to create new file with seq " + currentSeq);
                }
                rafActiveFile.getFD().sync();
                rafActiveFile.close();
                rafActiveFile = new RandomAccessFile(activeFile, "rw");
            }
            if (currentSeq.get() >= MAX_SEQ && !isCompacting.get()) {
                rafActiveFile.getFD().sync();
                isCompacting.set(true);
                new Thread(() -> {
                    try {
                        compact();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        isCompacting.set(false);
                    }
                }).start();
            }
        }
    }

    public String get(int key) throws IOException {
        synchronized (getLock) {
            FilePointer pointer = logIndex.get(key);
            if (pointer == null) {
                return null;
            }
            try (RandomAccessFile raf = new RandomAccessFile(parentPath + File.separator + pointer.getFileId() + EXTENSION, "r")) {
                raf.seek(pointer.getValueOffset());
                BitCaskFileEntry bitCaskFileEntry = fileOperations.readNextDataEntry(raf);
                return bitCaskFileEntry == null ? null : new String(bitCaskFileEntry.getValue());
            }
        }
    }


    private synchronized void compact() throws IOException {
        createCompactFiles();
        ConcurrentHashMap<Integer, FilePointer> compactedMap = createCompactedMap();
        synchronized (getLock) {
            deleteOldFiles();
            updateLogIndex(compactedMap);
            renameCompactFiles();
            renameRemainingFiles();
            synchronized (putLock) {
                renameActiveFileAndUpdateSeq();
            }
        }
        isCompacting.set(false);

    }

    private void renameActiveFileAndUpdateSeq() throws IOException {
        rafActiveFile.close();
        String newFileName = parentPath + File.separator + (currentSeq.get() - MAX_SEQ + 1) + EXTENSION;
        boolean renamed = fileOperations.renameFile(activeFile, newFileName);
        if (!renamed) {
            System.err.println("Failed to rename active file");
        }

        activeFile = new File(newFileName);
        rafActiveFile = new RandomAccessFile(activeFile, "rw");
        //update the log index of the old files that have  been renamed
        for (Map.Entry<Integer, FilePointer> entry : logIndex.entrySet()) {
            if (entry.getValue().getFileId() == currentSeq.get()) {
                long newFileId = currentSeq.get() - MAX_SEQ + 1;
                updateEntry(entry, newFileId);
            }
        }
        currentSeq.set(currentSeq.get() - MAX_SEQ + 1);
    }

    private void renameRemainingFiles() {
        File[] files = fileOperations.listFiles(parentPath);
        for (File file : files) {
            int seq = fileOperations.getSequenceOfFile(file);
            if (seq >= MAX_SEQ && seq < currentSeq.get()) {
                fileOperations.renameFile(file, parentPath + File.separator + (seq - MAX_SEQ + 1) + EXTENSION);
            }
        }
        for (Map.Entry<Integer, FilePointer> entry : logIndex.entrySet()) {
            if (entry.getValue().getFileId() >= MAX_SEQ && entry.getValue().getFileId() < currentSeq.get()) {
                long newFileId = (entry.getValue().getFileId() - MAX_SEQ + 1);
                updateEntry(entry, newFileId);
            }
        }
    }

    private void updateEntry(Map.Entry<Integer, FilePointer> entry, long newFileId) {
        long valueOffset = entry.getValue().getValueOffset();
        long valueSize = entry.getValue().getValueSize();
        long timestamp = entry.getValue().getTimestamp();
        FilePointer pointer = new FilePointer(newFileId, valueSize, valueOffset, timestamp);
        if (timestamp >= logIndex.get(entry.getKey()).getTimestamp()) {
            logIndex.put(entry.getKey(), pointer);
        }
    }

    private void updateLogIndex(Map<Integer, FilePointer> compactedMap) {
        for (Map.Entry<Integer, FilePointer> entry : compactedMap.entrySet()) {
            if (entry.getValue().getTimestamp() >= logIndex.get(entry.getKey()).getTimestamp()) {
                logIndex.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void createCompactFiles() throws IOException {
        File compactFile = new File(parentPath + File.separator + COMPACT_NAME + EXTENSION);
        File hintFile = new File(parentPath + File.separator + COMPACT_NAME + HINT_EXTENSION);
        boolean created = compactFile.createNewFile() && hintFile.createNewFile();
        if (!created) {
            System.err.println("Failed to create compact files");
        }
    }

    private ConcurrentHashMap<Integer, FilePointer> createCompactedMap() throws IOException {
        File compactFile = new File(parentPath + File.separator + COMPACT_NAME + EXTENSION);
        File hintFile = new File(parentPath + File.separator + COMPACT_NAME + HINT_EXTENSION);
        RandomAccessFile rafCompact = new RandomAccessFile(compactFile, "rw");
        RandomAccessFile rafHint = new RandomAccessFile(hintFile, "rw");
        ConcurrentHashMap<Integer, FilePointer> compactedMap = new ConcurrentHashMap<>();

        for (Map.Entry<Integer, FilePointer> entry : logIndex.entrySet()) {
            FilePointer pointer = new FilePointer(0, entry.getValue().getValueSize(), rafCompact.getFilePointer(), entry.getValue().getTimestamp());
            compactedMap.put(entry.getKey(), pointer);

            try (RandomAccessFile raf = new RandomAccessFile(parentPath + File.separator + entry.getValue().getFileId() + EXTENSION, "r")) {
                raf.seek(entry.getValue().getValueOffset());
                BitCaskFileEntry bitCaskFileEntry = fileOperations.readNextDataEntry(raf);

                // Debug statement to verify entries are read correctly
                System.out.println("Read from original file: Key=" + entry.getKey() + ", Value=" + new String(bitCaskFileEntry.getValue()));

                HintFileEntry hintFileEntry = new HintFileEntry(pointer.getTimestamp(), entry.getKey(), bitCaskFileEntry.getKeySize(), bitCaskFileEntry.getValueSize(), rafCompact.getFilePointer());
                fileOperations.writeDataEntry(rafCompact, bitCaskFileEntry);
                fileOperations.writeHintEntry(rafHint, hintFileEntry);

                // Debug statement to verify entries are written correctly to compact file
                System.out.println("Written to compact file: Key=" + entry.getKey() + ", Value=" + new String(bitCaskFileEntry.getValue()));
            }
        }
        rafCompact.getFD().sync();
        rafHint.getFD().sync();
        rafCompact.close();
        rafHint.close();
        return compactedMap;
    }


    private synchronized void deleteOldFiles() {
        File[] files = fileOperations.listFiles(parentPath);
        for (File file : files) {
            if (!file.getName().contains(COMPACT_NAME)) {
                int seq = fileOperations.getSequenceOfFile(file);
                boolean deleted = true;
                System.out.println("Deleting file: " + file.getName());
                if (seq == -1) {
                    System.err.println("Failed to get sequence of file: " + file.getName());
                }
                if (file.getName().endsWith(HINT_EXTENSION) && seq < MAX_SEQ) {
                    deleted = file.delete();
                } else if (file.getName().endsWith(EXTENSION) && seq < MAX_SEQ) {
                    deleted = file.delete();
                }
                if (!deleted) {
                    System.err.println("Failed to delete file: " + file.getName());
                }
            }
        }
    }

    private void renameCompactFiles() {
        File compactFile = new File(parentPath + File.separator + COMPACT_NAME + EXTENSION);
        File hintFile = new File(parentPath + File.separator + COMPACT_NAME + HINT_EXTENSION);
        boolean renamed = fileOperations.renameFile(compactFile, parentPath + File.separator + "0" + EXTENSION);
        renamed = renamed && fileOperations.renameFile(hintFile, parentPath + File.separator + "0" + HINT_EXTENSION);
        if (!renamed) {
            System.err.println("Failed to rename compact files");
        }
    }
}