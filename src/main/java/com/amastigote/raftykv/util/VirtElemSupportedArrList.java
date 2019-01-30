package com.amastigote.raftykv.util;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author: hwding
 * @date: 2019/1/27
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[V-LIST]")
public class VirtElemSupportedArrList<X> extends ArrayList<X> {
    private int virtSize;

    public VirtElemSupportedArrList(int virtSize) {
        resetVirtSize(virtSize);
    }

    public VirtElemSupportedArrList(Collection<? extends X> c, int virtSize) {
        super(c);
        resetVirtSize(virtSize);
    }

    private void resetVirtSize(int newVirtSize) {
        if (newVirtSize < 0) {
            throw new IllegalArgumentException("virtSize can not be negative!");
        }

        this.virtSize = newVirtSize;
    }

    public int totalSize() {
        return virtSize + super.size();
    }

    @Deprecated
    @Override
    public int size() {
        throw new UnsupportedOperationException("use virtualSize(), actualSize() or totalSize() instead!");
    }

    @Override
    public List<X> subList(int fromIndex, int toIndex) {
        return super.subList(fromIndex - virtSize, toIndex - virtSize);
    }
}
