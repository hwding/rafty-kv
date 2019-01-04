package com.amastigote.raftymicrocluster.disk;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.LogEntry;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: hwding
 * @date: 2019/1/4
 * <p>
 * <p>
 * This is a simple alterable (for term & vote) & appendable (for logs)
 * de/serializer for node state persistent.
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
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "SERIALIZER")
final class AppendableStateSerializer {
    private static final byte OP_PUT = 0x00;
    private static final byte OP_RMV = 0x0F;
    private final Object lock = new Object();
    private RandomAccessFile pFile;

    AppendableStateSerializer(RandomAccessFile pFile) {
        this.pFile = pFile;
    }

    void initFile() throws IOException {
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

    private int recoverInt(int offset) throws IOException {

        synchronized (lock) {
            pFile.seek(offset);
            return pFile.readInt();
        }
    }

    @SuppressWarnings({"PointlessArithmeticExpression", "PointlessBitwiseExpression"})
    private int recoverInt(byte[] bytes, int offset) {
        byte b0 = bytes[offset + 0];
        byte b1 = bytes[offset + 1];
        byte b2 = bytes[offset + 2];
        byte b3 = bytes[offset + 3];

        return ((b0 & 0xFF) << 24) | ((b1 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | ((b3 & 0xFF) << 0);
    }

    @SuppressWarnings({"PointlessArithmeticExpression", "PointlessBitwiseExpression"})
    private void writeInt(byte[] des, int offset, int data) {
        des[offset + 0] = (byte) ((data >>> 24) & 0xFF);
        des[offset + 1] = (byte) ((data >>> 16) & 0xFF);
        des[offset + 2] = (byte) ((data >>> 8) & 0xFF);
        des[offset + 3] = (byte) ((data >>> 0) & 0xFF);
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

                LogEntry.LogCommandType entryComType = (op == 0x00 ? LogEntry.LogCommandType.PUT : LogEntry.LogCommandType.REMOVE);
                LogEntry entry = new LogEntry();

                entry.setKey(entryKeyObj);
                entry.setTerm(entryTerm);
                entry.setLogCommandType(entryComType);

                if (entryComType.equals(LogEntry.LogCommandType.PUT)) {
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

    @SuppressWarnings({"Duplicates"})
    void persistLogEntry(LogEntry entry) throws IOException {
        LogEntry.LogCommandType commandType = entry.getLogCommandType();
        Object key = entry.getKey();
        Object val = entry.getValue();
        int term = entry.getTerm();

        byte[] buf;
        byte op = commandType.equals(LogEntry.LogCommandType.PUT) ? OP_PUT : OP_RMV;

        ByteArrayOutputStream byteArrOutStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutStream = new ObjectOutputStream(byteArrOutStream);

        objOutStream.flush();
        objOutStream.writeObject(key);
        byte[] keyBytes = byteArrOutStream.toByteArray();
        int keyLen = keyBytes.length;

        if (commandType.equals(LogEntry.LogCommandType.PUT)) {
            byteArrOutStream = new ByteArrayOutputStream();
            objOutStream = new ObjectOutputStream(byteArrOutStream);

            objOutStream.flush();
            objOutStream.writeObject(val);
            byte[] valBytes = byteArrOutStream.toByteArray();
            int valLen = valBytes.length;

            int bufLen = 1 + 4 + 4 + keyLen + 4 + valLen;
            buf = new byte[bufLen];

            buf[0] = op;

            writeInt(buf, 1, term);
            writeInt(buf, 5, keyLen);

            System.arraycopy(keyBytes, 0, buf, 9, keyLen);

            writeInt(buf, 9 + keyLen, valLen);

            System.arraycopy(valBytes, 0, buf, 9 + keyLen + 4, valLen);
        } else {
            int bufLen = 1 + 4 + 4 + keyLen;
            buf = new byte[bufLen];

            buf[0] = op;

            writeInt(buf, 1, term);
            writeInt(buf, 5, keyLen);

            System.arraycopy(keyBytes, 0, buf, 9, keyLen);
        }

        synchronized (lock) {
            pFile.seek(pFile.length());
            pFile.write(buf);
        }

        log.debug("entry serialized with buf len {}", buf.length);
    }
}
