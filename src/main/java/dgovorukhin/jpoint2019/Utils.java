package dgovorukhin.jpoint2019;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class Utils {

    private Utils() {

    }

    private static final String FREE_PAGE_CACHE_COMMAND = "echo 1 > /proc/sys/vm/drop_caches";

    public static void freePageCache() {
        Runtime rt = Runtime.getRuntime();

        Process prc = null;

        try {
            prc = rt.exec(FREE_PAGE_CACHE_COMMAND);
        }
        catch (IOException e) {

        }

        try {
            prc.waitFor();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return;
        }

        System.out.println(prc.getOutputStream());
        System.err.println(prc.getErrorStream());
    }

    public static final long GB_1 = 1024 * 1024 * 1024L;
    public static final long GB_10 = GB_1 * 10;
    public static final long GB_100 = GB_1 * 100;


    protected static void randomPayload(FileChannel ch, long size) throws IOException {
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
