package dgovorukhin.jpoint2019;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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

import static dgovorukhin.jpoint2019.Utils.GB_1;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class BenchmarkMmapvsBuffer {

    // 1 GB
    //Benchmark                                 Mode  Cnt  Score   Error  Units
    //BenchmarkMmapvsBuffer.doReadDirectBuffer    ss       0.656           s/op
    //BenchmarkMmapvsBuffer.doReadHeapBuffer      ss       1.038           s/op
    //BenchmarkMmapvsBuffer.doReadMmap            ss       0.575           s/op

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(BenchmarkMmapvsBuffer.class.getSimpleName())
            .threads(1)
            .warmupIterations(0)
            .measurementIterations(1)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    public static class AbstractContext {

        public FileChannel ch;

        public long size;

        public void doSetup() throws IOException {
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

            ByteBuffer buf = ByteBuffer.allocate(4096);

            long written = 0;

            size = GB_1 * 2;

            while (written < size) {
                while (buf.position() < buf.capacity())
                    buf.putLong(1L);

                buf.flip();

                while (buf.hasRemaining())
                    ch.write(buf);

                buf.clear();

                written += buf.capacity();
            }

            System.out.println(">>> written=" + written);

            ch.force(true);

            System.out.println("Do Setup finished");
        }

        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            FileChannel src = ch;

            if (src != null) {
                src.close();
                System.out.println("Closed src");
            }
        }
    }

    @State(Scope.Benchmark)
    public static class MmapContext extends AbstractContext {
        public MappedByteBuffer mmapBuf;

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            super.doSetup();

            mmapBuf = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);

            //mmapBuf.load();
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            super.doTearDown();
        }
    }

    public abstract static class BufferContext extends AbstractContext {
        protected ByteBuffer buf;

        abstract void initBuffer(long size);
    }

    @State(Scope.Benchmark)
    public static class HeapBufferContext extends BufferContext {
        @Override void initBuffer(long size) {
            buf = ByteBuffer.allocate((int)size);
        }

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            super.doSetup();

            initBuffer(4096);
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            super.doTearDown();
        }
    }

    @State(Scope.Benchmark)
    public static class DirectBufferContext extends BufferContext {
        @Override void initBuffer(long size) {
            buf = ByteBuffer.allocateDirect((int)size);
        }

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            super.doSetup();

            initBuffer(4096);
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            super.doTearDown();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void doReadHeapBuffer(HeapBufferContext heapBufferContext) throws IOException {
        doReadBuffered(heapBufferContext.ch, heapBufferContext.buf, heapBufferContext.size);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void doReadDirectBuffer(DirectBufferContext directBufferContext) throws IOException {
        doReadBuffered(directBufferContext.ch, directBufferContext.buf, directBufferContext.size);
    }

    private long doReadBuffered(FileChannel ch, ByteBuffer buf, long size) throws IOException {
        long read = 0;

        long res = 0L;

        do {
            while (buf.position() < buf.capacity()) {
                read += ch.read(buf, read);
            }

            buf.flip();

            while (buf.hasRemaining())
                res += buf.getLong();

            buf.clear();
        }
        while (read < size);

        return res;
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public long doReadMmap(MmapContext mmapContext) throws IOException {
        MappedByteBuffer mmapBuf = mmapContext.mmapBuf;

        long res = 0L;

        while (mmapBuf.position() < mmapBuf.capacity())
            res += mmapBuf.getLong();

        mmapBuf.rewind();

        return res;
    }
}
