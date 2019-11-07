package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class PutMessage {

    //记录的为第一条时间，后面的时间均通过256*block获得
    long firstTime = -1;
    //用于记录每个时间间隔256的时间，在全量t数组中的位置
    int[] timeOffsets = new int[4000000];//基本上都是3900000，4000000其实有点冒风险
    //用于存储全量t，记录的为偏移量
    byte[] delta = new byte[200000000];
    //第一条时间+256*block的时间，作为偏移的基准
    long baseTimeLast = -256;
    //一个线程内消息总条数
    int countNum = 0;
    //    long lastTime = -1;
    ThreadLocal<ReadMessageAvg> threadLocalAvg = new ThreadLocal<>();
    ThreadLocal<ReadMessage> threadLocalRead = new ThreadLocal<>();


    public int blockNumber = 0;
    public int writeNumberA = 0;
    public int writeNumberB = 0;


    private AsynchronousFileChannel fileChannelAT;
    private AsynchronousFileChannel fileChannelBody;

    private LinkedBlockingQueue<ByteBuffer> writeBufferATList = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ByteBuffer> writeBufferBodyList = new LinkedBlockingQueue<>();
    private long WRITE_TA_SIZE = (long) Constants.TA_SIZE * (long) Constants.BLOCK_NUM;
    private long WRITE_BODY_SIZE = (long) Constants.DATA_SIZE * (long) Constants.BLOCK_NUM;

    ByteBuffer writeBufferAT = ByteBuffer.allocateDirect((int) WRITE_TA_SIZE);
    ByteBuffer writeBufferBody = ByteBuffer.allocateDirect((int) WRITE_BODY_SIZE);
    long[] aInMemory = new long[Constants.A_IN_MEMORY_NUMBER];

    public int name;

    public PutMessage(int name) {
        int TA_SIZE = Constants.TA_SIZE;
        int DATA_SIZE = Constants.DATA_SIZE;
        int BLOCK_NUM = Constants.BLOCK_NUM;
        for (int i = 0; i < Constants.WRITE_BUFFER_NUM; i++) {
            try {
                writeBufferATList.put(ByteBuffer.allocateDirect(TA_SIZE * BLOCK_NUM));
                writeBufferBodyList.put(ByteBuffer.allocateDirect(DATA_SIZE * BLOCK_NUM));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.name = name;
        String location = Constants.location;
        Path ATFile = Paths.get(location + name + "AT.txt");
        Path BodyFile = Paths.get(location + name + "Body.txt");
        if (!Files.exists(ATFile)) {
            try {
                Files.createFile(ATFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!Files.exists(BodyFile)) {
            try {
                Files.createFile(BodyFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileChannelAT = AsynchronousFileChannel.open(ATFile, StandardOpenOption.WRITE);
            fileChannelBody = AsynchronousFileChannel.open(BodyFile, StandardOpenOption.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void put(Message message) {
        if (firstTime == -1) {
//            System.out.println(name+" T:"+message.getT()+" A:"+message.getA());
            firstTime = message.getT();
        }
        if (message.getT() - baseTimeLast >= 256) {
            blockNumber = (int) (message.getT() - firstTime) / 256;
            timeOffsets[blockNumber] = countNum;
            baseTimeLast = firstTime + 256 * blockNumber;
        }
        byte thisDelta = (byte) (message.getT() - (baseTimeLast));
        delta[countNum] = thisDelta;
        if (countNum < Constants.A_IN_MEMORY_NUMBER) {
            aInMemory[countNum] = message.getA();
        }
        writeBufferAT.putLong(message.getA());
        if (!writeBufferAT.hasRemaining()) {
            ByteBuffer saveAT = writeBufferAT;
            saveAT.flip();
            fileChannelAT.write(saveAT, (long) writeNumberA * WRITE_TA_SIZE, null, new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    try {
                        saveAT.clear();
                        writeBufferATList.put(saveAT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                }
            });
            writeNumberA++;
            try {
                writeBufferAT = writeBufferATList.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //body每次都写入，countnum是总数都要++
        writeBufferBody.put(message.getBody());
        countNum++;
        if (!writeBufferBody.hasRemaining()) {
            ByteBuffer saveBody = writeBufferBody;
            saveBody.flip();
            fileChannelBody.write(saveBody, (long) writeNumberB * WRITE_BODY_SIZE, null, new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    try {
                        saveBody.clear();
                        writeBufferBodyList.put(saveBody);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                }
            });
            writeNumberB++;
            try {
                writeBufferBody = writeBufferBodyList.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void clearBuffer() {
        int postionAT = writeBufferAT.position();
        int postionBody = writeBufferBody.position();
        if (postionAT > 0) {
//            System.out.println(name+" last t:"+(firstTime+256*blockNumber));
            ByteBuffer saveAT = writeBufferAT;
            saveAT.flip();
            fileChannelAT.write(saveAT, (long) writeNumberA * WRITE_TA_SIZE, null, new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    try {
                        saveAT.clear();
                        writeBufferAT.clear();
                        fileChannelAT.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                }
            });
        }
        if (postionBody > 0) {
            ByteBuffer saveBody = writeBufferBody;
            saveBody.flip();
            Future futureBody = fileChannelBody.write(saveBody, (long) writeNumberB * WRITE_BODY_SIZE);
            while (!futureBody.isDone()) {
            }
            saveBody.clear();
            writeBufferBody.clear();
            try {
                fileChannelBody.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        blockNumber++;
    }

}
