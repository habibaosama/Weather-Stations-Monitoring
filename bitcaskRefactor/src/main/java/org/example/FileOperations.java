package org.example;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Set;

import static org.example.Bitcask.EXTENSION;
import static org.example.Bitcask.HINT_EXTENSION;

public class FileOperations {


    public void createDirectory(String path) {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    private File createFile(String parentPath, String fileName) {
        return new File(parentPath + File.separator + fileName);
    }

    public File[] listFiles(String path) {
        File file = new File(path);
        return file.listFiles() == null ? new File[0] : file.listFiles();
    }

    public boolean canCovertToInt(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public int getSequenceOfFile(File file) {
        String name = file.getName();
        String seq = name.substring(0, name.indexOf('.'));
        if (canCovertToInt(seq)) {
            return Integer.parseInt(seq);
        }
        return -1;
    }

    public int getMaxSeq(File[] files) {
        int max = 0;
        for (File file : files) {
            int seq = getSequenceOfFile(file);
            if (seq > max) {
                max = seq;
            }
        }
        return max;
    }

    public boolean renameFile(File file, String newName) {
        return file.renameTo(new File(newName));
    }


    public HintFileEntry readNextHintEntry(RandomAccessFile raf) throws IOException {
        long timestamp = raf.readLong();
        int keySize = raf.readInt();
        long valueSize = raf.readLong();
        long valueOffset = raf.readLong();
        int key = raf.readInt();
        return new HintFileEntry(timestamp, key, keySize, valueSize, valueOffset);
    }

    public void writeHintEntry(RandomAccessFile raf, HintFileEntry entry) throws IOException {
        raf.writeLong(entry.getTimestamp());
        raf.writeInt(entry.getKeySize());
        raf.writeLong(entry.getValueSize());
        raf.writeLong(entry.getValueOffset());
        raf.writeInt(entry.getKey());

    }

    public void writeDataEntry(RandomAccessFile raf, BitCaskFileEntry entry) throws IOException {
        raf.writeLong(entry.getTimestamp());
        raf.writeInt(entry.getKeySize());
        raf.writeLong(entry.getValueSize());
        raf.writeInt(entry.getKey());
        byte[] value = entry.getValue();
        raf.write(value);

    }

    public BitCaskFileEntry readNextDataEntry(RandomAccessFile raf) throws IOException {
        try {
            long timestamp = raf.readLong();
            int keySize = raf.readInt();
            long valueSize = raf.readLong();
            int key = raf.readInt();
            byte[] value = new byte[(int) valueSize];
            raf.read(value);
            return new BitCaskFileEntry(timestamp, keySize, valueSize, key, value);
        } catch (IOException e) {
            System.err.println("Error reading data file entry. File may be corrupted.");
            return null;
        }
    }


    public void readHintFile(File file, Map<Integer, FilePointer> logIndex) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            int fileId = getSequenceOfFile(file);
            while (raf.getFilePointer() < raf.length()) {
                HintFileEntry entry = readNextHintEntry(raf);
                FilePointer pointer = new FilePointer(fileId, entry.getValueSize(), entry.getValueOffset(), entry.getTimestamp());
                if (logIndex.containsKey(entry.getKey())) {
                    FilePointer existingPointer = logIndex.get(entry.getKey());
                    if (existingPointer.getTimestamp() <= pointer.getTimestamp()) {
                        logIndex.put(entry.getKey(), pointer);
                    }
                } else {
                    logIndex.put(entry.getKey(), pointer);
                }
            }
        }
    }

    public void readDataFile(File file, Map<Integer, FilePointer> logIndex) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            int fileId = getSequenceOfFile(file);
            while (raf.getFilePointer() < raf.length()) {
                long offset = raf.getFilePointer();
                BitCaskFileEntry entry = readNextDataEntry(raf);
                FilePointer pointer = new FilePointer(fileId, entry.getValueSize(), offset, entry.getTimestamp());
                if (logIndex.containsKey(entry.getKey())) {
                    FilePointer existingPointer = logIndex.get(entry.getKey());
                    if (existingPointer.getTimestamp() <= pointer.getTimestamp()) {
                        logIndex.put(entry.getKey(), pointer);
                    }
                } else {
                    logIndex.put(entry.getKey(), pointer);
                }
            }
        }
    }

    public void recoverHintFiles(File[] files, Set<Integer> fileIds, Map<Integer, FilePointer> logIndex) throws IOException {
        for (File file : files) {
            if (file.getName().endsWith(HINT_EXTENSION)) {
                int fileId = getSequenceOfFile(file);
                if (!fileIds.contains(fileId)) {
                    readHintFile(file, logIndex);
                    fileIds.add(fileId);
                }
            }
        }
    }

    public void recoverDataFiles(File[] files, Set<Integer> fileIds, Map<Integer, FilePointer> logIndex) throws IOException {
        for (File file : files) {
            if (file.getName().endsWith(EXTENSION)) {
                int fileId = getSequenceOfFile(file);
                if (!fileIds.contains(fileId)) {
                    readDataFile(file, logIndex);
                }
            }
        }
    }


}
