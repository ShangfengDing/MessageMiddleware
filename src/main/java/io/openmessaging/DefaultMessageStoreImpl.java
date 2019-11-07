package io.openmessaging;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private ConcurrentHashMap<Thread, PutMessage> putMessage = new ConcurrentHashMap<>();

    private volatile AtomicInteger name = new AtomicInteger(0);
    private volatile boolean inited = false;


    @Override
    public void put(Message message) {

        if (!putMessage.containsKey(Thread.currentThread())) {
            putMessage.put(Thread.currentThread(), new PutMessage(name.getAndIncrement()));
        }
        putMessage.get(Thread.currentThread()).put(message);
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        ArrayList<Message> result = new ArrayList<>();
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    init();
                    inited = true;
                }
            }
        }
        for (PutMessage messageThread : putMessage.values()) {
            if (messageThread.threadLocalRead.get() == null){
                messageThread.threadLocalRead.set(new ReadMessage(messageThread.name));
            }
            ReadMessage readMessage = messageThread.threadLocalRead.get();
            result.addAll(readMessage.getMessage(aMin, aMax, tMin, tMax, messageThread));
        }
        if(result.size()>0){
            Collections.sort(result, Comparator.comparingLong(Message::getT));
        }
        return result;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        long number = 0;
        for (PutMessage messageThread : putMessage.values()) {
            if (messageThread.threadLocalAvg.get()==null){
                messageThread.threadLocalAvg.set(new ReadMessageAvg(messageThread.name));
            }
            ReadMessageAvg readMessage = messageThread.threadLocalAvg.get();
            long[] result = readMessage.getAvgMessage(aMin, aMax, tMin, tMax, messageThread);
            sum += result[0];
            number += result[1];
        }
        return sum/number;
    }

    /**
     * 清空写入时候最后剩余的buffer内，未落盘的内容
     */
    private void init() {
        for (PutMessage putMessage : putMessage.values()) {
            putMessage.clearBuffer();
        }
    }

}
