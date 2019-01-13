package com.amastigote.raftykv.disk;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.protocol.LogEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author: hwding
 * @date: 2019/1/4
 * <p>
 * <p>
 * This is a simple alterable (for term & vote) & appendable (for logs)
 * de/serializer for node state persistent.
 * <p>
 * A cache is used to make truncate more efficient by
 * saving nearby entry idx locations in file as checkpoints
 * to speed up searching.
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
    private LocationCheckpointCache cache;

    private int lastPersistedEntryIdx = -1;

    AppendableStateSerializer(RandomAccessFile pFile, int checkpointCacheSize) {
        this.pFile = pFile;
        this.cache = new LocationCheckpointCache(checkpointCacheSize);
    }

    private void reset() {
        /* evict all cache */
        cache.evict(0);

        /* reset counter */
        lastPersistedEntryIdx = -1;
    }

    void initFile() throws IOException {
        log.warn("init persist file, this could imply a former persist failure...");
        synchronized (lock) {

            /* truncate to empty */
            pFile.setLength(0);

            /* write header with init val */
            persistCurTerm(NodeState.INIT_CUR_TERM);
            persistVotedFor(NodeState.INIT_VOTED_FOR);

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

    @SuppressWarnings("Duplicates")
    List<LogEntry> recoverEntries() throws Exception {
        this.reset();

        List<LogEntry> entries = new ArrayList<>();
        int readLen;
        long entryPos;

        synchronized (lock) {
            pFile.seek(8);
            while (true) {
                entryPos = pFile.getFilePointer();

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

                /* build cache while recovering */
                ++lastPersistedEntryIdx;
                cache.put(lastPersistedEntryIdx, entryPos);
            }
        }
        return entries;
    }

    @SuppressWarnings("Duplicates")
    void truncateLogEntry(final int toIdxExclusive) throws Exception {

        if (toIdxExclusive > lastPersistedEntryIdx) {
            log.warn("no need to truncate, entry not included in persist file, be aware that this could not happen");
            return;
        }

        int nextIdx;

        synchronized (lock) {
            int readLen;

            /* load nearest checkpoint from cache */
            LocationCheckpointCache.Result res = cache.get(toIdxExclusive);
            if (Objects.nonNull(res)) {
                pFile.seek(res.getPos());
                nextIdx = res.getIdx();

                log.info("cache hit at checkpoint {}", res);
            } else {
                pFile.seek(8);
                nextIdx = 0;

                log.info("cache miss");
            }

            while (nextIdx < toIdxExclusive) {
                byte[] partEntryHead = new byte[1 + 4 + 4];

                readLen = pFile.read(partEntryHead);
                if (readLen <= 0) {
                    break;
                }

                byte op = partEntryHead[0];
                int entryKeyLen = recoverInt(partEntryHead, 1 + 4);

                pFile.seek(pFile.getFilePointer() + entryKeyLen);

                LogEntry.LogCommandType entryComType = (op == 0x00 ? LogEntry.LogCommandType.PUT : LogEntry.LogCommandType.REMOVE);

                if (entryComType.equals(LogEntry.LogCommandType.PUT)) {
                    byte[] valLenBuf = new byte[4];

                    readLen = pFile.read(valLenBuf);
                    if (readLen != 4) {
                        throw new Exception("val len corrupted at pos " + pFile.getFilePointer());
                    }

                    int entryValLen = recoverInt(valLenBuf, 0);
                    pFile.seek(pFile.getFilePointer() + entryValLen);
                }

                /* already reach the end of file */
                if (pFile.getFilePointer() >= pFile.length()) {
                    log.warn("no need to truncate, entry not included in persist file, be aware that this could not happen");
                    return;
                }

                ++nextIdx;
            }

            long curPos = pFile.getFilePointer();
            pFile.setLength(curPos);

            lastPersistedEntryIdx = nextIdx - 1;
            cache.evict(toIdxExclusive);

            log.debug("log file truncated to idx inclusive {}, current file len {}", lastPersistedEntryIdx, pFile.length());
        }
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
            long entryPos = pFile.length();
            pFile.seek(entryPos);
            pFile.write(buf);

            ++lastPersistedEntryIdx;
            cache.put(lastPersistedEntryIdx, entryPos);
        }

        log.debug("entry persisted with buf len {}, current file len {}", buf.length, pFile.length());
    }

    /**
     * Cache based on a ring buffer to save
     * checkpoints up to _cacheSize_.
     * <p>
     * <p>
     * Since truncation mostly happens very near
     * the entries rear, cache will store continuous
     * _cacheSize_ entries succeeding to the rear.
     * <p>
     * All caches are evicted at least cost when
     * there's a truncate operation.
     * <p>
     * Make sure _cacheSize_ is not too large.
     */
    @Slf4j(topic = "CACHE")
    private static final class LocationCheckpointCache {

        private static int INIT_VAL = -1;
        /* log idx in state machine */
        private final int[] idxArr;
        /* related position in persist file */
        private final long[] posArr;
        @Getter
        private int cacheSize;
        private int next = 0;

        private LocationCheckpointCache(int cacheSize) {
            this.cacheSize = cacheSize;
            this.idxArr = new int[cacheSize];
            this.posArr = new long[cacheSize];

            for (int i = 0; i < cacheSize; ++i) {
                idxArr[i] = INIT_VAL;
                posArr[i] = INIT_VAL;
            }

            log.info("check point cache init with size {}", cacheSize);
        }

        /* build cache when:
         *      recover from disk
         *      persist new entry
         */
        private synchronized void put(int idx, long pos) {
            idxArr[next] = idx;
            posArr[next] = pos;

            next = (++next) % cacheSize;
        }

        /**
         * @param idealIdx idx we want to locate
         * @return nearest idx location & pos before the ideal idx, null if not found
         */
        private synchronized Result get(int idealIdx) {
            int prev = (next + cacheSize - 1) % cacheSize;

            do {
                if (idxArr[prev] <= idealIdx) {
                    return new Result(idxArr[prev], posArr[prev]);
                }

                prev = (prev + cacheSize - 1) % cacheSize;
            } while (prev != next && idxArr[prev] != INIT_VAL);

            return null;
        }

        private synchronized void evict(int toIdxExclusive) {
            final int init = (next + cacheSize - 1) % cacheSize;
            int prev = init;

            do {
                if (idxArr[prev] < toIdxExclusive) {
                    break;
                }

                idxArr[prev] = INIT_VAL;
                posArr[prev] = INIT_VAL;

                prev = (prev + cacheSize - 1) % cacheSize;
            } while (prev != init && idxArr[prev] != INIT_VAL);

            next = (++prev) % cacheSize;
        }

        @AllArgsConstructor
        @Getter
        @ToString
        private static final class Result {
            private int idx;
            private long pos;
        }
    }
}
