package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.protocol.LogEntry;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.amastigote.raftymicrocluster.protocol.LogEntry.LogCommandType;

/**
 * @author: hwding
 * @date: 2019/1/2
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "PERSIST")
final class PersistManager {
    private final static String CONFIGURATION_PREFIX = "com.amastigote.raftymicrocluster.";
    private final static String CONFIGURATION_FILE = "rafty-persist.properties";
    private final static String KEY_END_PERSIST_DIR = "persistDir";
    private final static String KEY_PERSIST_DIR = CONFIGURATION_PREFIX + KEY_END_PERSIST_DIR;
    private final static String DEFAULT_VAL_PERSIST_DIR = ".";
    private static AppendableStateSerializer stateSerializer;
    private final Properties properties = new Properties();
    private boolean recoverable = true;

    PersistManager() {
        try {
            loadProperties();

            String persistDir = properties.getProperty(KEY_PERSIST_DIR, DEFAULT_VAL_PERSIST_DIR);

            File file = new File(persistDir, String.valueOf(NodeStatus.nodePort()));
            if (!file.exists()) {
                boolean created = file.createNewFile();

                if (!created) {
                    throw new IOException("persist file failed to create");
                }

                log.info("persist file newly created");
                recoverable = false;
            }

            RandomAccessFile persistFile = new RandomAccessFile(file, "rws");

            stateSerializer = new AppendableStateSerializer(persistFile);

            if (!recoverable) {
                stateSerializer.initFile();
            }
        } catch (IOException e) {
            log.error("persist file failed to init", e);
            System.exit(-1);
        }
    }

    private void loadProperties() {
        InputStream in = AccessController.doPrivileged((PrivilegedAction<InputStream>) () -> {
            ClassLoader threadCL = Thread.currentThread().getContextClassLoader();
            if (threadCL != null) {
                return threadCL.getResourceAsStream(CONFIGURATION_FILE);
            } else {
                return ClassLoader.getSystemResourceAsStream(CONFIGURATION_FILE);
            }
        });
        if (null != in) {
            try {
                properties.load(in);
            } catch (java.io.IOException ignored) {
            } finally {
                try {
                    in.close();
                } catch (java.io.IOException ignored) {
                }
            }
        }
    }

    boolean recover() {
        if (!recoverable) {
            return false;
        }

        int term, vote;
        List<LogEntry> entries;
        try {
            term = stateSerializer.recoverCurTerm();
            vote = stateSerializer.recoverVotedFor();
            entries = stateSerializer.recoverEntries();
        } catch (Exception e) {
            log.error("error during recover, give up", e);
            return false;
        }

        NodeStatus.updateTerm(term);
        NodeStatus.voteFor(vote);
        NodeStatus.appendEntryUnaltered(entries);

        log.info("recovered with term {}, votedFor {}, entries {}", term, vote, entries.size());
        return true;
    }

    void persistCurrentTerm(int term) {
        try {
            stateSerializer.persistCurTerm(term);
        } catch (IOException e) {
            log.warn("failed to persist curTerm {}", term);
        }
    }

    void persistVotedFor(int votedFor) {
        try {
            stateSerializer.persistVotedFor(votedFor);
        } catch (IOException e) {
            log.warn("failed to persist votedFor {}", votedFor);
        }
    }

    void persistLogEntry(LogEntry entry) {
        try {
            stateSerializer.persistLogEntry(entry);
        } catch (IOException e) {
            log.warn("failed to persist log entry {}", entry);
        }
    }

    /**
     * This is a simple alterable (for term & vote) & appendable (for logs)
     * de/serializer for node state persistent.
     * <p>
     * <p>
     * Data is persisted in byte arr, format (unit is byte):
     * <p>
     * --------HEAD---------
     * 1) len 4, current-term
     * 2) len 4, voted-for
     * ------LOG-ENTRY------ (repeatedly)
     * 3) len 1, log op, 0x00 for PUT, not 0x00 (use 0x0F) for REMOVE
     * 4) len 4, entry's term
     * 5 PUT & REMOVE) len 4, entry key length
     * 6 PUT & REMOVE) len ?, entry key content
     * 7 PUT         ) len 4, entry val length
     * 8 PUT         ) len ?, entry val content
     * ------LOG-ENTRY------ (repeatedly)
     * ...
     */
    private static final class AppendableStateSerializer {
        private final Object lock = new Object();

        private final byte OP_PUT = 0x00;
        private final byte OP_RMV = 0x0F;
        private RandomAccessFile pFile;

        private AppendableStateSerializer(RandomAccessFile pFile) {
            this.pFile = pFile;
        }

        private void initFile() throws IOException {
            log.warn("init persist file, this could imply a former persist failure...");
            synchronized (lock) {

                /* truncate to empty */
                pFile.setLength(0);

                /* write header with init val */
                persistCurTerm(NodeStatus.INIT_CUR_TERM);
                persistVotedFor(NodeStatus.INIT_VOTED_FOR);

                log.debug("new persist file init to pos {}", pFile.length());
            }
            log.info("persist file init ok");
        }

        void persistCurTerm(int newCurTerm) throws IOException {
            synchronized (lock) {
                pFile.seek(0);
                pFile.writeInt(newCurTerm);
            }
        }

        void persistVotedFor(int newVotedFor) throws IOException {
            synchronized (lock) {
                pFile.seek(4);
                pFile.writeInt(newVotedFor);
            }
        }

        int recoverCurTerm() throws IOException {
            return recoverInt(0);
        }

        int recoverVotedFor() throws IOException {
            return recoverInt(4);
        }

        int recoverInt(int offset) throws IOException {

            synchronized (lock) {
                pFile.seek(offset);
                return pFile.readInt();
            }
        }

        @SuppressWarnings({"PointlessArithmeticExpression", "PointlessBitwiseExpression"})
        int recoverInt(byte[] bytes, int offset) {
            return (bytes[offset + 0] << 24)
                    + (bytes[offset + 1] << 16)
                    + (bytes[offset + 2] << 8)
                    + (bytes[offset + 3] << 0);
        }

        List<LogEntry> recoverEntries() throws Exception {
            List<LogEntry> entries = new ArrayList<>();
            int readLen;

            synchronized (lock) {
                pFile.seek(8);
                while (true) {
                    byte[] partEntryHead = new byte[1 + 4 + 4];

                    readLen = pFile.read(partEntryHead);
                    if (readLen <= 0) {
                        break;
                    }

                    byte op = partEntryHead[0];
                    int entryTerm = recoverInt(partEntryHead, 1);
                    int entryKeyLen = recoverInt(partEntryHead, 1 + 4);

                    byte[] entryKeyBytes = new byte[entryKeyLen];

                    readLen = pFile.read(entryKeyBytes);
                    if (readLen != entryKeyLen) {
                        throw new Exception("key corrupted at pos " + pFile.getFilePointer());
                    }

                    ByteArrayInputStream byteArrInStream = new ByteArrayInputStream(entryKeyBytes);
                    ObjectInputStream objInStream = new ObjectInputStream(byteArrInStream);

                    Object entryKeyObj = objInStream.readObject();
                    objInStream.close();
                    byteArrInStream.close();

                    LogCommandType entryComType = (op == 0x00 ? LogCommandType.PUT : LogCommandType.REMOVE);
                    LogEntry entry = new LogEntry();

                    entry.setKey(entryKeyObj);
                    entry.setTerm(entryTerm);
                    entry.setLogCommandType(entryComType);

                    if (entryComType.equals(LogCommandType.PUT)) {
                        byte[] valLenBuf = new byte[4];

                        readLen = pFile.read(valLenBuf);
                        if (readLen != 4) {
                            throw new Exception("val len corrupted at pos " + pFile.getFilePointer());
                        }

                        int entryValLen = recoverInt(valLenBuf, 0);

                        byte[] entryValBytes = new byte[entryValLen];

                        readLen = pFile.read(entryValBytes);
                        if (readLen != entryValLen) {
                            throw new Exception("val corrupted at pos " + pFile.getFilePointer());
                        }

                        byteArrInStream = new ByteArrayInputStream(entryValBytes);
                        objInStream = new ObjectInputStream(byteArrInStream);

                        Object entryValObj = objInStream.readObject();
                        objInStream.close();
                        byteArrInStream.close();

                        entry.setValue(entryValObj);
                    }
                    entries.add(entry);
                }
            }
            return entries;
        }

        @SuppressWarnings({"Duplicates", "PointlessArithmeticExpression", "PointlessBitwiseExpression"})
        void persistLogEntry(LogEntry entry) throws IOException {
            LogCommandType commandType = entry.getLogCommandType();
            Object key = entry.getKey();
            Object val = entry.getValue();
            int term = entry.getTerm();

            byte[] buf;
            byte op = commandType.equals(LogCommandType.PUT) ? OP_PUT : OP_RMV;

            ByteArrayOutputStream byteArrOutStream = new ByteArrayOutputStream();
            ObjectOutputStream objOutStream = new ObjectOutputStream(byteArrOutStream);

            objOutStream.flush();
            objOutStream.writeObject(key);
            byte[] keyBytes = byteArrOutStream.toByteArray();
            int keyLen = keyBytes.length;

            if (commandType.equals(LogCommandType.PUT)) {
                byteArrOutStream = new ByteArrayOutputStream();
                objOutStream = new ObjectOutputStream(byteArrOutStream);

                objOutStream.flush();
                objOutStream.writeObject(val);
                byte[] valBytes = byteArrOutStream.toByteArray();
                int valLen = valBytes.length;

                int bufLen = 1 + 4 + 4 + keyLen + 4 + valLen;
                buf = new byte[bufLen];

                buf[0] = op;

                buf[1] = (byte) ((term >>> 24) & 0xFF);
                buf[2] = (byte) ((term >>> 16) & 0xFF);
                buf[3] = (byte) ((term >>> 8) & 0xFF);
                buf[4] = (byte) ((term >>> 0) & 0xFF);

                buf[5] = (byte) ((keyLen >>> 24) & 0xFF);
                buf[6] = (byte) ((keyLen >>> 16) & 0xFF);
                buf[7] = (byte) ((keyLen >>> 8) & 0xFF);
                buf[8] = (byte) ((keyLen >>> 0) & 0xFF);

                System.arraycopy(keyBytes, 0, buf, 9, keyLen);

                buf[9 + keyLen + 0] = (byte) ((valLen >>> 24) & 0xFF);
                buf[9 + keyLen + 1] = (byte) ((valLen >>> 16) & 0xFF);
                buf[9 + keyLen + 2] = (byte) ((valLen >>> 8) & 0xFF);
                buf[9 + keyLen + 3] = (byte) ((valLen >>> 0) & 0xFF);

                System.arraycopy(valBytes, 0, buf, 9 + keyLen + 4, valLen);
            } else {
                int bufLen = 1 + 4 + 4 + keyLen;
                buf = new byte[bufLen];

                buf[0] = op;

                buf[1] = (byte) ((term >>> 24) & 0xFF);
                buf[2] = (byte) ((term >>> 16) & 0xFF);
                buf[3] = (byte) ((term >>> 8) & 0xFF);
                buf[4] = (byte) ((term >>> 0) & 0xFF);

                buf[5] = (byte) ((keyLen >>> 24) & 0xFF);
                buf[6] = (byte) ((keyLen >>> 16) & 0xFF);
                buf[7] = (byte) ((keyLen >>> 8) & 0xFF);
                buf[8] = (byte) ((keyLen >>> 0) & 0xFF);

                System.arraycopy(keyBytes, 0, buf, 9, keyLen);
            }

            synchronized (lock) {
                pFile.seek(pFile.length());
                pFile.write(buf);
            }

            log.debug("entry serialized with buf len {}", buf.length);
        }
    }
}
