package dgovorukhin.jpoint2019;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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

import static dgovorukhin.jpoint2019.Utils.randomPayload;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class BenchmarkDirectvsHeapBufferCopy {
    // 4kb
    //530780.273          ops/s heap
    //572147.154          ops/s direct

    // 8kb
    //321314.311         ops/s heap
    //4389545.997          ops/s direct

    // 16kb
    //215315.824          ops/s heap
    //271425.203          ops/s direct

    // 32kb
    //134497.580          ops/s heap
    //169896.952          ops/s direct

    // 64kb
    //78575.852          ops/s heap
    //99333.937          ops/s direct

    // 128kb
    //37437.151          ops/s heap
    //49165.052          ops/s direct

    private static final long GB_1 = 1024 * 1024 * 1024L;

    private static final int BUFFER_SIZE = 4096 * 4;

    private static final int ITER = (int)(GB_1 / BUFFER_SIZE) / 2;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(BenchmarkDirectvsHeapBufferCopy.class.getSimpleName())
            .threads(1)
            .warmupIterations(1)
            .measurementIterations(2)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class Context {

        private ByteBuffer buf;

        private FileChannel ch;

        private long pos;

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            System.out.println("Do Setup");

            buf = ByteBuffer.allocateDirect(4096 * 32);
            //buf = ByteBuffer.allocateDirect(4096 * 32 );

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

            randomPayload(ch, GB_1);

            pos = ch.size() - buf.capacity();
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            FileChannel src = ch;

            if (src != null) {
                src.close();
                System.out.println("Closed src");
            }

            pos = 0;
        }

        @TearDown(Level.Iteration)
        public void reset() throws IOException {
            System.out.println("Do reset");

            pos = ch.size() - buf.capacity();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void doCopy(Context context) throws IOException {
        ByteBuffer buf = context.buf;

        if (context.pos < 0)
            context.pos = context.ch.size() - buf.capacity();

        int read = 0;

        while (read < buf.capacity())
            read += context.ch.read(buf, context.pos);

        buf.clear();

        context.pos -= buf.capacity();
    }
}
