package dgovorukhin.jpoint2019;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import static dgovorukhin.jpoint2019.Utils.GB_1;
import static dgovorukhin.jpoint2019.Utils.GB_10;
import static dgovorukhin.jpoint2019.Utils.GB_100;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class BenchmarkTransferFileRunner {

    private static final String SRC = "src.bin";
    private static final String DEST = "dest.bin";

    public static void main(String[] args) throws Exception {

//        byte[] buf = new byte[4096];
//
//        try (FileInputStream is = new FileInputStream(new File(path))) {
//            is.read(buf);
//        }
//
//        try (FileOutputStream is = new FileOutputStream(new File(path))) {
//            is.write(buf);
//        }

        //org.openjdk.jmh.Main.main(args);

        System.out.println("Do Setup");
        //>>> completed, 595 msec
        //>>> completed, 62294 msec
        //>>> completed, 705057 msec
        TransferMode mode = TransferMode.MMAP;

        //>>> completed, 1135 msec
        //>>> completed, 16929 msec
        //>>> completed, 193487 msec
        //TransferMode mode = TransferMode.ARBITRARY;
        long size = GB_1 / 4;

        System.out.println(">>> mode=" + mode + ", size=" + size);

        String dirPath = System.getProperty("user.dir");

        File tmpDir = new File(dirPath, "tmpjpoint2019");

        if (!tmpDir.exists())
            tmpDir.mkdir();
        else
            for (File f : tmpDir.listFiles())
                f.delete();

        File srcFile = new File(tmpDir, SRC);
        File destFile = new File(tmpDir, DEST);

        srcFile.createNewFile();
        destFile.createNewFile();

        System.out.println(">>> src=" + srcFile);
        System.out.println(">>> dest=" + destFile);

        FileChannel src = FileChannel.open(srcFile.toPath(), WRITE, READ);
        FileChannel dest = FileChannel.open(destFile.toPath(), WRITE);

        System.out.println(">>> random load src file");

        WritableByteChannel dest_w = null;

        FileContext.randomPayload(src, size);

        switch (mode) {
            case DIRECT:
                // No-op.
                break;
            case MMAP:
                src = mode.disableDirect(src);
                break;

            case ARBITRARY:
                src = mode.disableDirect(src);
                dest_w = mode.disableMMAP((WritableByteChannel)dest);
                break;
        }

        long time = System.currentTimeMillis();

        long copied = 0;

        int iter = 0;

        while (copied < size) {
            long t = System.currentTimeMillis();

            long written = src.transferTo(copied, size, dest_w != null ? dest_w : dest);

            System.out.println("iter=" + iter + ", time=" + (System.currentTimeMillis() - t) + ", bytes=" + written);

            copied += written;

            iter++;
        }

        dest.force(true);

        System.out.println(">>> completed, " +
            ((System.currentTimeMillis() - time)) + " msec");

        System.out.println(">>> src size=" + src.size() + ", dest size=" + dest.size() + ", iter=" + iter);
    }

    @State(Scope.Benchmark)
    public static class FileContext {
        private FileChannel src;
        private FileChannel dest;
        private WritableByteChannel dest_w;
        private TransferMode mode;
        private long size;

        @Param({"MMAP"})
        private String mode_param;

        @Param({"GB_1"})
        private String size_param;

        private long size() {
            switch (size_param) {
                case "GB_1":
                    return GB_1;
                case "GB_10":
                    return GB_10;
                case "GB_100":
                    return GB_100;
                default:
                    throw new UnsupportedOperationException("Unspported size:" + size_param);
            }
        }

        private TransferMode mode() {
            return TransferMode.valueOf(mode_param);
        }

        @Setup(Level.Iteration)
        public void doSetup() throws IOException {
            System.out.println("Do Setup");

            mode = mode();
            size = size();

            System.out.println(">>> mode=" + mode + ", size=" + size);

            String dirPath = System.getProperty("user.dir");

            File tmpDir = new File(dirPath, "tmpjpoint2019");

            if (!tmpDir.exists())
                tmpDir.mkdir();
            else
                for (File f : tmpDir.listFiles())
                    f.delete();

            File srcFile = new File(tmpDir, SRC);
            File destFile = new File(tmpDir, DEST);

            srcFile.createNewFile();
            destFile.createNewFile();

            System.out.println(">>> src=" + srcFile);
            System.out.println(">>> dest=" + destFile);

            src = FileChannel.open(srcFile.toPath(), WRITE, READ);
            dest = FileChannel.open(destFile.toPath(), WRITE);

            System.out.println(">>> random load src file");

            long time = System.currentTimeMillis();

            randomPayload(src, size);

            System.out.println(">>> completed, " +
                ((System.currentTimeMillis() - time) / 1000) + " sec");

            switch (mode) {
                case DIRECT:
                    // No-op.
                    break;
                case MMAP:
                    src = mode.disableDirect(src);
                    break;

                case ARBITRARY:
                    src = mode.disableDirect(src);
                    dest_w = mode.disableMMAP((WritableByteChannel)dest);
                    break;
            }

            //Utils.freePageCache();
        }

        @TearDown(Level.Iteration)
        public void doTearDown() throws IOException {
            System.out.println("Do TearDown");

            FileChannel src0 = src;

            if (src0 != null) {
                src0.close();
                System.out.println("Closed src");
            }

            FileChannel dest0 = dest;

            if (dest0 != null) {
                dest0.close();
                System.out.println("Closed dest");
            }
        }

        private static void randomPayload(FileChannel ch, long size) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(4 * 4096);

            Random rnd = new Random();

            long written = 0;

            while (written < size) {
                rnd.nextBytes(buf.array());

                while (buf.hasRemaining())
                    ch.write(buf);

                buf.clear();

                written += buf.capacity();
            }

            System.out.println(">>> written=" + written);

            ch.force(true);
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 0)
    @BenchmarkMode(Mode.SingleShotTime)
    public void transferTo(FileContext fileContext) throws IOException {
        FileChannel src = fileContext.src;
        WritableByteChannel desc = fileContext.dest_w != null ? fileContext.dest_w : fileContext.dest;

        long postion = 0;

        while (postion < fileContext.size)
            postion += src.transferTo(postion, fileContext.size, desc);
    }

    private enum TransferMode {
        DIRECT,
        MMAP,
        ARBITRARY;

        private FileChannel disableDirect(FileChannel ch) {
            String fieldName = "transferSupported";

            Class<? extends FileChannel> cls = ch.getClass();

            try {
                Field field = cls.getDeclaredField(fieldName);

                field.setAccessible(true);

                field.set(ch, false);
            }
            catch (IllegalAccessException | NoSuchFieldException e) {

            }

            return ch;
        }

        private WritableByteChannel disableMMAP(final WritableByteChannel ch) {
            return new WritableByteChannel() {
                @Override public int write(ByteBuffer src) throws IOException {
                    return ch.write(src);
                }

                @Override public boolean isOpen() {
                    return ch.isOpen();
                }

                @Override public void close() throws IOException {
                    ch.close();
                }
            };
        }

        private ReadableByteChannel disableMMAP(final ReadableByteChannel ch) {
            return new ReadableByteChannel() {
                @Override public int read(ByteBuffer dst) throws IOException {
                    return ch.read(dst);
                }

                @Override public boolean isOpen() {
                    return ch.isOpen();
                }

                @Override public void close() throws IOException {
                    ch.close();
                }
            };
        }
    }
}
