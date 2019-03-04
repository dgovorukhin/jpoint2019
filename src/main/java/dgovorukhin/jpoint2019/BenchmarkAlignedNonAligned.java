package dgovorukhin.jpoint2019;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import one.nio.os.Mem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import sun.nio.ch.FileChannelImpl;

import static dgovorukhin.jpoint2019.Utils.GB_1;
import static dgovorukhin.jpoint2019.Utils.randomPayload;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class BenchmarkAlignedNonAligned {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(BenchmarkAlignedNonAligned.class.getSimpleName())
            .threads(1)
            .warmupIterations(0)
            .measurementIterations(1)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    //BenchmarkAlignedNonAligned.randomReadWrite    ss       297.803           s/op
    //BenchmarkAlignedNonAligned.randomReadWrite    ss         319.098            s/op

    //>>> 942741
    //>>> 1444120

    @State(Scope.Benchmark)
    public static class ReadWriteContext {

        public FileChannel ch;

        public long size;

        public ByteBuffer readBuf;

        private long[] readPositions;

        @Setup(Level.Trial)
        public void doSetup() throws IOException, NoSuchFieldException, IllegalAccessException {
            System.out.println("Do Setup start");

            String dirPath = System.getProperty("user.dir");

            File tmpDir = new File(dirPath, "tmpjpoint2019");

            if (!tmpDir.exists())
                tmpDir.mkdir();
            else
                for (File f : tmpDir.listFiles())
                    f.delete();

            File srcFile = new File(tmpDir, "src");

            srcFile.createNewFile();

            ch = FileChannel.open(srcFile.toPath(), WRITE, READ);

            size = GB_1 * 8;

            randomPayload(ch, size);

            ch.position(0);

            readBuf = ByteBuffer.allocateDirect(4096);

            List<Long> pos1 = new ArrayList<>((int)(size / 4096));

            for (long i = 0; i < size; i += 4096)
                pos1.add(i);

//            for (long i = 2048; i < size - 1; i += 4096)
//                pos1.add(i);

            Collections.shuffle(pos1);

            this.readPositions = pos1.stream().mapToLong(Long::longValue).toArray();

//            Field fdf = ch.getClass().getDeclaredField("fd");
//
//            fdf.setAccessible(true);
//
//            FileDescriptor fd = (FileDescriptor)fdf.get(ch);
//
//            Mem.posix_fadvise(fd, 0, size, Mem.POSIX_FADV_RANDOM);

            File tmpFile = new File(tmpDir, "src");

            tmpFile.createNewFile();

            randomPayload(FileChannel.open(tmpFile.toPath(), WRITE, READ), GB_1 * 8);
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            readBuf.clear();

            FileChannel src = ch;

            if (src != null) {
                src.close();

                System.out.println("Closed src");
            }
        }
    }

    //    @Benchmark
//    @BenchmarkMode(Mode.SingleShotTime)
    public void randomReadWrite(ReadWriteContext readWriteContext) throws IOException {
        FileChannel ch = readWriteContext.ch;
        ByteBuffer readBuf = readWriteContext.readBuf;

        long[] r_pos = readWriteContext.readPositions;

        for (int i = 0; i < r_pos.length; i++) {
            long readPostition = r_pos[i];

            int readBytes = ch.read(readBuf, readPostition);

            if (readBytes == -1)
                break;

            readBuf.clear();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void readN(ReadWriteContext readWriteContext) throws IOException {
        FileChannel ch = readWriteContext.ch;
        ByteBuffer readBuf = readWriteContext.readBuf;

        long[] r_pos = readWriteContext.readPositions;

        int idx = ThreadLocalRandom.current().nextInt(r_pos.length - 1);

        long pos = r_pos[idx];

        pos += 2048;

        while (readBuf.position() < readBuf.capacity()) {
            long time = System.nanoTime();

            int readBytes = ch.read(readBuf, pos);

            System.out.println(">>> " + (System.nanoTime() - time) + " " + pos);

            if (readBytes == -1)
                break;
        }

        readBuf.clear();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void readA(ReadWriteContext readWriteContext) throws IOException {
        FileChannel ch = readWriteContext.ch;
        ByteBuffer readBuf = readWriteContext.readBuf;

        long[] r_pos = readWriteContext.readPositions;

        int idx = ThreadLocalRandom.current().nextInt(r_pos.length - 1);

        long pos = r_pos[idx];

        while (readBuf.position() < readBuf.capacity()) {
            long time = System.nanoTime();

            int readBytes = ch.read(readBuf, pos);

            System.out.println(">>> " + (System.nanoTime() - time) + " " + pos);

            if (readBytes == -1)
                break;
        }

        readBuf.clear();
    }
}
