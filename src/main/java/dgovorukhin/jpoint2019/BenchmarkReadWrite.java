package dgovorukhin.jpoint2019;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.runner.options.TimeValue;

import static dgovorukhin.jpoint2019.Utils.GB_1;
import static dgovorukhin.jpoint2019.Utils.randomPayload;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class BenchmarkReadWrite {

    //READ

    // 1GB
    //Benchmark                      Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomRead    ss       0.624           s/op
    //BenchmarkReadWrite.seqRead       ss       0.536           s/op

    // 2GB
    //Benchmark                      Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomRead    ss       1.346           s/op
    //BenchmarkReadWrite.seqRead       ss       1.133           s/op

    // 4GB
    //Benchmark                      Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomRead    ss       2.518           s/op
    //BenchmarkReadWrite.seqRead       ss       1.897           s/op

    // 8GB
    //Benchmark                      Mode  Cnt   Score   Error  Units
    //BenchmarkReadWrite.randomRead    ss       91.161           s/op
    //BenchmarkReadWrite.seqRead       ss        5.050           s/op

    // 16GB
    //Benchmark                      Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.randomRead    ss       591.275           s/op
    //BenchmarkReadWrite.seqRead       ss        12.279           s/op

    //WRITE

    // 1GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.appendOnlyWrite    ss       2.000           s/op
    //BenchmarkReadWrite.randomWrite        ss       1.691           s/op

    // 2GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.appendOnlyWrite    ss       2.848           s/op
    //BenchmarkReadWrite.randomWrite        ss       4.493           s/op

    // 4GB
    //Benchmark                           Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.appendOnlyWrite    ss         5.524           s/op
    //BenchmarkReadWrite.randomWrite        ss       211.990           s/op

    // 8GB
    //Benchmark                           Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.appendOnlyWrite    ss        13.987           s/op
    //BenchmarkReadWrite.randomWrite        ss       588.857           s/op

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(BenchmarkReadWrite.class.getSimpleName())
            .threads(1)
            .warmupIterations(0)
            .measurementIterations(1)
            .timeout(new TimeValue(1, TimeUnit.HOURS))
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class RandomReadContext {

        public FileChannel ch;

        public long size;

        public ByteBuffer buf;

        private long[] pos;

        @Setup(Level.Trial)
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

            size = GB_1;

            randomPayload(ch, size);

            ch.position(0);

            buf = ByteBuffer.allocateDirect(4096);

            List<Long> pos = new ArrayList<>((int)(size / 4096));

            for (long i = 0; i < size; i += 4096)
                pos.add(i);

            Collections.shuffle(pos);

            this.pos = pos.stream().mapToLong(Long::longValue).toArray();

            //System.out.println(Arrays.toString(this.pos));

            System.out.println("Do Setup finished");
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            buf.clear();

            FileChannel src = ch;

            if (src != null) {
                src.close();

                System.out.println("Closed src");
            }
        }
    }

    //    @Benchmark
//    @BenchmarkMode(Mode.SingleShotTime)
    public void randomRead(RandomReadContext rwContext) throws IOException {
        FileChannel ch = rwContext.ch;
        ByteBuffer buf = rwContext.buf;
        long[] pos = rwContext.pos;

        long totalRead = 0;
        long size = rwContext.size;
        long procent = size / 100;

        for (int i = 0; i < pos.length; i++) {
            int read = 0;

            while (buf.position() < buf.capacity()) {
                read += ch.read(buf, pos[i] + buf.position());
            }

            if (read == -1)
                break;

            totalRead += read;

            if (totalRead % procent == 0)
                System.out.println(">>> read " + totalRead);

            buf.clear();
        }

        // System.out.println(">>> total read=" + totalRead);
    }

    @State(Scope.Benchmark)
    public static class ReadContext {

        public FileChannel ch;

        public long size;

        public ByteBuffer buf;

        @Setup(Level.Trial)
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

            size = GB_1;

            randomPayload(ch, size);

            ch.position(0);

            buf = ByteBuffer.allocateDirect(4096);

            System.out.println("Do Setup finished");
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            buf.clear();

            FileChannel src = ch;

            if (src != null) {
                src.close();

                System.out.println("Closed src");
            }
        }
    }

    //    @Benchmark
//    @BenchmarkMode(Mode.SingleShotTime)
    public void seqRead(ReadContext rwContext) throws IOException {
        FileChannel ch = rwContext.ch;
        ByteBuffer buf = rwContext.buf;

        long totalRead = 0;
        long size = rwContext.size;

        long procent = size / 100;

        while (true) {
            int read = 0;

            while (buf.position() < buf.capacity()) {
                int bytes = ch.read(buf);

                if (bytes == -1) {
                    // System.out.println("chPos=" + ch.position());

                    read = -1;

                    break;
                }

                read += bytes;
            }

            //System.out.println(">>> pos=" + totalRead);

            if (read == -1)
                break;

            totalRead += read;

            buf.clear();

            if (totalRead % procent == 0)
                System.out.println(">>> read " + totalRead);
        }

        ch.position(0);

        //System.out.println(">>> total read=" + totalRead);
    }

    @State(Scope.Benchmark)
    public static class RandomWriteContext {

        public FileChannel ch;

        public long size;

        public ByteBuffer buf;

        private long[] pos;

        @Setup(Level.Trial)
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

            size = GB_1 * 16;

            randomPayload(ch, size);

            ch.position(0);

            buf = ByteBuffer.allocateDirect(4096);

            List<Long> pos = new ArrayList<>((int)(size / 4096));

            for (long i = 0; i < size; i += 4096)
                pos.add(i);

            Collections.shuffle(pos);

            Random rnd = new Random();

            byte[] payload = new byte[buf.capacity()];

            rnd.nextBytes(payload);

            buf.put(payload);

            buf.flip();

            this.pos = pos.stream().mapToLong(Long::longValue).toArray();

            //System.out.println(Arrays.toString(this.pos));

            System.out.println("Do Setup finished");
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            buf.clear();

            FileChannel src = ch;

            if (src != null) {
                src.close();

                System.out.println("Closed src");
            }
        }
    }

    //    @Benchmark
//    @BenchmarkMode(Mode.SingleShotTime)
    public void randomWrite(RandomWriteContext rwContext) throws IOException {
        FileChannel ch = rwContext.ch;
        ByteBuffer buf = rwContext.buf;
        long[] pos = rwContext.pos;

        long totalWritten = 0;
        long size = rwContext.size;
        long procent = size / 100;

        for (int i = 0; i < pos.length; i++) {
            int written = 0;

            while (buf.position() < buf.capacity()) {
                written += ch.write(buf, pos[i] + buf.position());
            }

            if (written == -1)
                break;

            totalWritten += written;

            if (totalWritten % procent == 0)
                System.out.println(">>> written " + totalWritten);

            buf.rewind();
        }

    }

    @State(Scope.Benchmark)
    public static class WriteContext {

        public FileChannel ch;

        public long size;

        public ByteBuffer buf;

        @Setup(Level.Trial)
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

            size = GB_1 * 16;

            randomPayload(ch, size);

            ch.position(0);

            buf = ByteBuffer.allocateDirect(4096);

            Random rnd = new Random();

            byte[] payload = new byte[buf.capacity()];

            rnd.nextBytes(payload);

            buf.put(payload);

            buf.flip();

            System.out.println("Do Setup finished");
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            buf.clear();

            FileChannel src = ch;

            if (src != null) {
                src.close();

                System.out.println("Closed src");
            }
        }
    }

    //    @Benchmark
//    @BenchmarkMode(Mode.SingleShotTime)
    public void appendOnlyWrite(WriteContext writeContext) throws IOException {
        FileChannel ch = writeContext.ch;
        ByteBuffer buf = writeContext.buf;

        long size = writeContext.size;

        long totalWritten = 0L;

        while (totalWritten < size) {
            int written = 0;

            while (buf.position() < buf.capacity()) {
                int bytes = ch.write(buf);

                if (bytes == -1) {
                    written = -1;

                    break;
                }

                written += bytes;
            }

            totalWritten += written;

            if (written == -1)
                break;

            buf.rewind();
        }

        ch.position(0);
    }

    @State(Scope.Benchmark)
    public static class ReadWriteContext {

        public FileChannel ch;

        public long size;

        public ByteBuffer readBuf;
        public ByteBuffer writeBuf;

        private long[] readPositions;
        private long[] writePositions;

        @Setup(Level.Trial)
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

            size = GB_1 * 8;

            randomPayload(ch, size);

            ch.position(0);

            readBuf = ByteBuffer.allocateDirect(4096);
            writeBuf = ByteBuffer.allocateDirect(4096);

            List<Long> pos1 = new ArrayList<>((int)(size / 4096));

            for (long i = 0; i < size / 5; i += 4096)
                pos1.add(i);

            Collections.shuffle(pos1);

            this.readPositions = pos1.stream().mapToLong(Long::longValue).toArray();

            List<Long> pos2 = new ArrayList<>((int)(size / 4096));

            for (long i = 0; i < size / 5 * 4; i += 4096)
                pos2.add(i);

            Collections.shuffle(pos2);

            this.writePositions = pos2.stream().mapToLong(Long::longValue).toArray();

            Random rnd = new Random();

            byte[] payload = new byte[writeBuf.capacity()];

            rnd.nextBytes(payload);

            writeBuf.put(payload);

            writeBuf.flip();

            System.out.println("Do Setup finished r=" + readPositions.length + " w=" + writePositions.length);
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            readBuf.clear();
            writeBuf.clear();

            FileChannel src = ch;

            if (src != null) {
                src.close();

                System.out.println("Closed src");
            }
        }
    }
    // 80/20

    // 2GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       1.711           s/op

    // 4GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       3.429           s/op

    // 8GB
    //Benchmark                           Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       204.676           s/op

    // 50/50

    // 2GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       2.276           s/op

    // 4GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       5.125           s/op

    // 8GB
    //Benchmark                           Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       335.226           s/op

    // 20/80

    // 2GB
    //Benchmark                           Mode  Cnt  Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       2.730           s/op

    // 4GB
    //Benchmark                           Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       136.871           s/op

    // 8GB
    //Benchmark                           Mode  Cnt    Score   Error  Units
    //BenchmarkReadWrite.randomReadWrite    ss       523.254           s/op

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void randomReadWrite(ReadWriteContext readWriteContext) throws IOException {
        FileChannel ch = readWriteContext.ch;
        ByteBuffer readBuf = readWriteContext.readBuf;
        ByteBuffer writeBuf = readWriteContext.writeBuf;

        long size = readWriteContext.size;

        long[] r_pos = readWriteContext.readPositions;
        long[] w_pos = readWriteContext.writePositions;

        int portion;
        int iter = 0;

        int lastIdx = 0;

        int totalReads = 0;
        int totalWrites = 0;

        if (r_pos.length >= w_pos.length) {
            portion = (r_pos.length / w_pos.length);

            for (int i = 0; i < r_pos.length; i++) {
                long readPostition = r_pos[i];

                int readBytes = ch.read(readBuf, readPostition);

                if (readBytes == -1)
                    break;

                readBuf.clear();

                totalReads++;

                iter++;

                if (iter == portion && lastIdx < w_pos.length) {
                    long writePosition = w_pos[lastIdx];

                    int writeBytes = ch.write(writeBuf, writePosition);

                    totalWrites++;

                    writeBuf.rewind();

                    lastIdx++;

                    iter = 0;
                }
            }
        }
        else {
            portion = (w_pos.length / r_pos.length);

            for (int i = 0; i < w_pos.length; i++) {
                long writePosition = w_pos[i];

                int writeBytes = ch.write(writeBuf, writePosition);

                if (writeBytes == -1)
                    break;

                writeBuf.rewind();

                totalWrites++;

                iter++;

                if (iter == portion && lastIdx < r_pos.length) {
                    long readPostition = r_pos[lastIdx];

                    int readBytes = ch.read(readBuf, readPostition);

                    readBuf.clear();

                    totalReads++;

                    lastIdx++;

                    iter = 0;
                }
            }
        }

        System.out.println("Total r=" + totalReads + " w=" + totalWrites);
    }
}
