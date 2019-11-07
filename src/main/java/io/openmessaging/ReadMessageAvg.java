package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ReadMessageAvg {

    private FileChannel fileChannelAT;
    private FileChannel fileChannelBody;
    RandomAccessFile ATFile;
    RandomAccessFile BodyFile;
    BuffersAvg buffersAvg = new BuffersAvg();

    /**
     * 索引内部间隔
     * 大块内部的小块间隔
     * 大块：写入时，多少条AT写入一次，即为大块
     * 小块：写入时，写一次AT大块时，会在中间记录多次T，并且存入Index索引，为小块
     */


    public ReadMessageAvg(int name) {
        String location = Constants.location;
        try {
            ATFile = new RandomAccessFile(location + name + "AT.txt", "rw");
            BodyFile = new RandomAccessFile(location + name + "Body.txt", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fileChannelAT = ATFile.getChannel();
        fileChannelBody = BodyFile.getChannel();
    }

    public long[] getAvgMessage(long aMin, long aMax, long tMin, long tMax, PutMessage messageThread) {
        long[] result = new long[2];
        boolean isAllMemory = false;
        long totalA = 0;
        int numberA = 0;
        result[0] = totalA;
        result[1] = numberA;
        try {

            /**
             * 找到大块在index数组中的左边界和右边界
             */
            int beginPosition = (int) (tMin - messageThread.firstTime) / 256;
            int endPosition = (int) (tMax - messageThread.firstTime) / 256;

            //怕有间断问题
            int beginOffset = messageThread.timeOffsets[beginPosition];
            while (beginOffset == 0 && beginPosition > 0) {
                if (beginPosition > messageThread.blockNumber) {
                    return result;
                }
                beginPosition++;
                beginOffset = messageThread.timeOffsets[beginPosition];
            }

            int nextBeginPosition = beginPosition + 1;
            int beginOffsetNext = messageThread.timeOffsets[nextBeginPosition];
            while (beginOffsetNext == 0) {
                if (nextBeginPosition > messageThread.blockNumber) {
                    beginOffsetNext = messageThread.countNum;
                    break;
                }
                nextBeginPosition++;
                beginOffsetNext = messageThread.timeOffsets[nextBeginPosition];
            }

            long beginOffsetTime = messageThread.firstTime + 256 * beginPosition;
            int beginIndex;
            if(tMin<=beginOffsetTime){
                beginIndex = beginOffset;
            }else {
                beginIndex = binarySearchMax((int) (tMin - beginOffsetTime), messageThread.delta, beginOffset, beginOffsetNext - 1);
            }
            int endIndex;
            if (endPosition > messageThread.blockNumber) {
                //                endPosition = messageThread.blockNumber-1;
                endIndex = messageThread.countNum-1;
            } else {
                int endOffset = messageThread.timeOffsets[endPosition];
                while (endOffset == 0 && endPosition > 0) {
                    endPosition--;
                    endOffset = messageThread.timeOffsets[endPosition];
                }

                int nextEndPosition = endPosition + 1;
                int endOffsetNext = messageThread.timeOffsets[nextEndPosition];
                while (endOffsetNext == 0) {
                    if (nextEndPosition > messageThread.blockNumber) {
                        endOffsetNext = messageThread.countNum;
                        break;
                    }
                    nextEndPosition++;
                    endOffsetNext = messageThread.timeOffsets[nextEndPosition];
                }

                long endOffsetTime = messageThread.firstTime + 256 * endPosition;
                long endOffsetTimeNext = messageThread.firstTime + 256 * nextEndPosition;
                if (tMax > endOffsetTimeNext) {
                    endIndex = endOffsetNext;
                }else {
                    endIndex = binarySearch((int) (tMax - endOffsetTime), messageThread.delta, endOffset, endOffsetNext-1);
                }
            }
            int TA_SIZE = Constants.TA_SIZE;
//            ByteBuffer byteBufferAT = ByteBuffer.allocateDirect(TA_SIZE * (endIndex - beginIndex+1));
            ByteBuffer byteBufferAT = buffersAvg.byteBufferAT;
            int A_IN_MEMORY_NUMBER = Constants.A_IN_MEMORY_NUMBER;
            if (beginIndex>=A_IN_MEMORY_NUMBER){
                byteBufferAT.limit(TA_SIZE * (endIndex - beginIndex+1));
                fileChannelAT.read(byteBufferAT, (long) beginIndex * (long) TA_SIZE);
                byteBufferAT.flip();
            }else if(beginIndex<A_IN_MEMORY_NUMBER && endIndex>A_IN_MEMORY_NUMBER){
                byteBufferAT.limit(TA_SIZE * (endIndex - A_IN_MEMORY_NUMBER+1));
                fileChannelAT.read(byteBufferAT, (long) A_IN_MEMORY_NUMBER * (long) TA_SIZE);
                byteBufferAT.flip();
            }else {
                byteBufferAT.limit(100);
                isAllMemory = true;
            }

            int currentIndex = beginIndex;
            int nextIndex = messageThread.blockNumber;
            int addNumber = 0;
            for(int temp=beginPosition+1;temp<messageThread.blockNumber+1;temp++){
                if(messageThread.timeOffsets[temp]!=0){
                    nextIndex = messageThread.timeOffsets[temp];
                    addNumber = temp - beginPosition;
                    break;
                }
            }
            while (byteBufferAT.position() < byteBufferAT.limit()) {
                long a;
                if(currentIndex<A_IN_MEMORY_NUMBER){
                   a = messageThread.aInMemory[currentIndex];
                }else{
                    a = byteBufferAT.getLong();
                }
                if (currentIndex == nextIndex) {
                    beginPosition+=addNumber;
                    beginOffsetTime += 256*addNumber;
                    for(int temp=beginPosition+1;temp<messageThread.blockNumber+1;temp++){
                        if(messageThread.timeOffsets[temp]!=0){
                            nextIndex = messageThread.timeOffsets[temp];
                            addNumber = temp - beginPosition;
                            break;
                        }
                    }
                }
                long t = beginOffsetTime + (messageThread.delta[currentIndex]& 0xFF);
                if (a >= aMin && a <= aMax && t>=tMin && t<=tMax) {
                    totalA+=a;
                    numberA++;
                }
                currentIndex++;
                if (isAllMemory && currentIndex >= endIndex+1){
                    break;
                }
            }
            byteBufferAT.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
        result[0] = totalA;
        result[1] = numberA;
        return result;
    }

    public int binarySearch(int time, byte[] index, int low2, int up2) {
        int low = low2;
        int up = up2;
        int ans = low2;
        while (up >= low) {
            int mid = low + (up - low) / 2;
            if ((index[mid] & 0xFF) > time) {
                up = mid - 1;
            } else {
                ans = mid; ///对下取整
                low = mid + 1;
            }
        }
        return ans;
    }

    public int binarySearchMax(int time, byte[] index, int low2, int up2) {
        if ((index[up2]& 0xFF) < time) return up2;
        int lo = low2, hi = up2;
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if ((index[mid]& 0xFF) < time) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    public static void main(String[] args) {
        int time = 7;
        byte[] index = {1, 3, 3,3,4, 6, 8, 10, 12};
//        int result = binarySearchMax(time, index, 0, 8);
//        System.out.println(result);
    }

}