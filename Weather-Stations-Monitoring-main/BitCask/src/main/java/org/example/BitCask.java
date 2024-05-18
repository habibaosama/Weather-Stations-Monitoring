package org.example;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.lang.System.err;

class BitCask {
    private static final int MAX_FILE_SIZE = 1024; // 1GB
    private static final int MAX_SEQ = 100;
    private static final String EXTENSION = ".BitCask";
    private HashMap<Integer, FilePointer> logIndex;
    private DataOutputStream dataOutputStream;
    private File activeFile;
    private long sequenceNumber = 0;

    public BitCask() {
        logIndex = new HashMap<>(Short.MAX_VALUE);
    }

    private DataOutputStream createDataOutputStream(File file) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, true)));
    }

    private DataInputStream createDataInputStream(File file) throws IOException {
        return new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    }

    private boolean isHintFile(String name) {
        return name.endsWith(".Hint.BitCask");
    }

    private boolean isBitCaskFile(String fileName) {
        return fileName.endsWith(EXTENSION);
    }

    private File getFileWithFileId(long fileId) {
        return new File(activeFile.getParent() + "/" + fileId + EXTENSION);
    }

    private void writeHintToStream(DataOutputStream stream, HintFileEntry hintFileEntry) throws IOException {
        stream.writeInt(hintFileEntry.getKeySize());
        stream.writeLong(hintFileEntry.getValueSize());
        stream.writeInt(hintFileEntry.getKey());
        stream.writeLong(hintFileEntry.getValueOffset());
    }

    private void writeEntryToStream(DataOutputStream stream, BitCaskFileEntry entry) throws IOException {
        stream.writeInt(entry.getKeySize());
        stream.writeLong(entry.getValueSize());
        stream.writeInt(entry.getKey());
        stream.write(entry.getValue());
    }

    private BitCaskFileEntry readEntryFromStream(DataInputStream dataInputStream) {
        try {
            int keySize = dataInputStream.readInt();
            long valueSize = dataInputStream.readLong();
            int key = dataInputStream.readInt();
            byte[] value = new byte[(int) valueSize];
            long read = dataInputStream.read(value);
            if (read != valueSize) {
                err.println("Failed to read the correct number of bytes");
            }
            return new BitCaskFileEntry(keySize, valueSize, key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void open(String path) {
        File dir = createDirectory(path);
        File[] files = getFiles(dir);
        int[] activeAndLogFileIndex = findLatestFileIndex(files);
        try {
            setActiveFile(path, files, activeAndLogFileIndex);
            updateLogIndex(files);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void updateLogIndexFromHintFile(File file) {
        try (DataInputStream hintStream = createDataInputStream(file)) {
            while (hintStream.available() > 0) {
                HintFileEntry hintFileEntry = readHintFromStream(hintStream);
                if (hintFileEntry != null) {
                    logIndex.put(hintFileEntry.getKey(), new FilePointer(extractFileSequence(file.getName()), hintFileEntry.getValueSize(), hintFileEntry.getValueOffset()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateLogIndexFromBitCaskFile(File file, HashSet<Integer> fileIds, long bytesRead) {
        fileIds.add((int) extractFileSequence(file.getName()));
        try (DataInputStream dataInputStream = createDataInputStream(file)) {
            while (dataInputStream.available() > 0) {
                BitCaskFileEntry entry = readEntryFromStream(dataInputStream);
                if (entry != null) {
                    logIndex.put(entry.getKey(), new FilePointer(extractFileSequence(file.getName()), entry.getValueSize(), bytesRead));
                    bytesRead = bytesRead + 4 + 8 + 4 + entry.getValue().length;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateLogIndex(File[] files) {
        long MaxFileId = -1;
        long bytesRead;
        HashSet<Integer> fileIds = new HashSet<>();
        for (File file : files) {
            bytesRead = 0;
            if (isHintFile(file.getName()) && extractFileSequence(file.getName()) > MaxFileId) {
                fileIds.add((int) extractFileSequence(file.getName()));
                MaxFileId = extractFileSequence(file.getName());
                updateLogIndexFromHintFile(file);
            } else if (isBitCaskFile(file.getName()) && extractFileSequence(file.getName()) > MaxFileId && !fileIds.contains((int) extractFileSequence(file.getName()))) {
                updateLogIndexFromBitCaskFile(file, fileIds, bytesRead);
                MaxFileId = extractFileSequence(file.getName());
            }
        }
    }

    private HintFileEntry readHintFromStream(DataInputStream hintStream) {
        try {
            int keySize = hintStream.readInt();
            long valueSize = hintStream.readLong();
            int key = hintStream.readInt();
            long valueOffset = hintStream.readLong();
            return new HintFileEntry(key, keySize, valueSize, valueOffset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private File createDirectory(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            boolean success = dir.mkdir();
            if (!success) {
                err.println("Failed to create directory: " + dir.getAbsolutePath());
            }
        }
        return dir;
    }

    private File[] getFiles(File dir) {
        File[] files = dir.listFiles();
        return files != null ? files : new File[0];
    }

    private int[] findLatestFileIndex(File[] files) {
        if (files.length == 0) {
            return new int[]{0, 0};
        }
        int latestFileIndex = 0;
        long latestFileSequence = -1;

        for (int index = 0; index < files.length; index++) {
            File currentFile = files[index];
            String fileName = currentFile.getName();
            long fileSequence = extractFileSequence(fileName);
            if (isBitCaskFile(fileName) && fileSequence > latestFileSequence) {
                latestFileIndex = index;
                latestFileSequence = fileSequence;
            }
        }
        return new int[]{latestFileIndex};
    }


    private long extractFileSequence(String fileName) {
        String[] fileNameParts = fileName.split("\\.");
        if (fileNameParts.length > 0 && canConvertToLong(fileNameParts[0])) {
            return Long.parseLong(fileNameParts[0]);
        }
        return 0;
    }

    private void setActiveFile(String path, File[] files, int[] activeAndLogFileIndex) throws IOException {
        if (files.length > 0) {
            activeFile = files[activeAndLogFileIndex[0]];
            sequenceNumber = extractFileSequence(activeFile.getName());
        } else {
            activeFile = new File(path + "/" + sequenceNumber + EXTENSION);
            createNewFiles();
        }
        dataOutputStream = createDataOutputStream(activeFile);
    }

    private void createNewFiles() {
        try {
            boolean success = activeFile.createNewFile();
            if (!success) {
                err.println("Failed to create new file: " + activeFile.getAbsolutePath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean canConvertToLong(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public synchronized void put(Integer key, String value) {
        byte[] valueBytes = value.getBytes();
        BitCaskFileEntry entry = new BitCaskFileEntry(4, valueBytes.length, key, valueBytes);
        FilePointer filePointer = new FilePointer(sequenceNumber, entry.getValueSize(), activeFile.length());
        logIndex.put(key, filePointer);
        try {
            writeEntryToStream(dataOutputStream, entry);
            dataOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (activeFile.length() >= MAX_FILE_SIZE) {
            ++sequenceNumber;
            activeFile = new File(activeFile.getParent() + "/" + sequenceNumber + EXTENSION);
            createNewFiles();
            try {
                dataOutputStream.flush();
                dataOutputStream.close();
                dataOutputStream = createDataOutputStream(activeFile);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if (sequenceNumber >= MAX_SEQ) {
            compaction();
        }

    }


    public String get(Integer key) {
        FilePointer filePointer = logIndex.get(key);
        if (filePointer == null) {
            return null;
        }
        File file = getFileWithFileId(filePointer.getFileId());
        try (DataInputStream dataInputStream = createDataInputStream(file)) {
            long skip = dataInputStream.skip(filePointer.getValueOffset());
            long skipLog = filePointer.getValueOffset();
            if (skip != skipLog) {
                err.println("Failed to skip the correct number of bytes");
            }
            BitCaskFileEntry entry = readEntryFromStream(dataInputStream);
            if (entry == null || entry.getKey() != key) {
                return null;
            }
            return new String(entry.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private synchronized void compaction() {
        //deep copy the HashMap to avoid compaction of the active file
        try {
            dataOutputStream.flush();
            dataOutputStream.close();
            mergeOldFiles(logIndex);
            sequenceNumber = 1;
            activeFile = new File(activeFile.getParent() + "/" + sequenceNumber + EXTENSION);
            createNewFiles();
            dataOutputStream = createDataOutputStream(activeFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mergeOldFiles(HashMap<Integer, FilePointer> logIndex) throws IOException {
        BitCaskFileEntry[] nBits = getOldEntries(logIndex);
        deleteOldFiles();
        File newFile = new File(activeFile.getParent() + "/" + 0 + EXTENSION);
        File hintFile = new File(activeFile.getParent() + "/" + 0 + ".Hint.BitCask");
        DataOutputStream newDataOutputStream = createDataOutputStream(newFile);
        DataOutputStream hintDataOutputStream = createDataOutputStream(hintFile);
        for (BitCaskFileEntry nBit : nBits) {
            FilePointer filePointer = new FilePointer(0, nBit.getValueSize(), newFile.length());
            HintFileEntry hintFileEntry = new HintFileEntry(nBit.getKey(), nBit.getKeySize(), nBit.getValueSize(), newFile.length());
            writeEntryToStream(newDataOutputStream, nBit);
            writeHintToStream(hintDataOutputStream, hintFileEntry);
            logIndex.put(nBit.getKey(), filePointer);
        }
        newDataOutputStream.flush();
        newDataOutputStream.close();
        hintDataOutputStream.flush();
        hintDataOutputStream.close();
    }


    private BitCaskFileEntry[] getOldEntries(HashMap<Integer, FilePointer> oldLogIndex) {
        BitCaskFileEntry[] nBits = new BitCaskFileEntry[oldLogIndex.size()];
        int index = 0;
        for (Map.Entry<Integer, FilePointer> entry : oldLogIndex.entrySet()) {
            File file = getFileWithFileId(entry.getValue().getFileId());
            try (DataInputStream oldFileStream = createDataInputStream(file)) {
                long skip = oldFileStream.skip(entry.getValue().getValueOffset());
                if (skip != entry.getValue().getValueOffset()) {
                    err.println("Failed to skip the correct number of bytes");
                }
                BitCaskFileEntry bitCaskFileEntry = readEntryFromStream(oldFileStream);
                nBits[index++] = bitCaskFileEntry;
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (!file.exists()) {
                err.println("File does not exist: " + file.getAbsolutePath());
            }
        }
        return nBits;
    }

    private void deleteOldFiles() {
        File[] files = getFiles(activeFile.getParentFile());
        for (File file : files) {
            if ((isBitCaskFile(file.getName()) || isHintFile(file.getName())) && extractFileSequence(file.getName()) <= sequenceNumber) {
                boolean success = file.delete();
                if (!success) {
                    err.println("Failed to delete file: " + file.getAbsolutePath());
                }
            }

        }
    }

    public void close() {
        try {
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}