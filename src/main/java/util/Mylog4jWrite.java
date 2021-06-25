package util;

import org.apache.log4j.DailyRollingFileAppender;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

/**
 * ClassName Mylog4jWrite
 * Date 2019/12/26 15:09
 **/
public class Mylog4jWrite extends DailyRollingFileAppender {
    @Override
    public synchronized void setFile(String fileName, boolean append,
                                     boolean bufferedIO, int bufferSize) throws IOException {
        super.setFile(fileName, append, bufferedIO, bufferSize);
        File f = new File(fileName);
        Set<PosixFilePermission> set = new HashSet<PosixFilePermission>();
        set.add(PosixFilePermission.OWNER_READ);
        set.add(PosixFilePermission.OTHERS_WRITE);
        set.add(PosixFilePermission.GROUP_READ);
        set.add(PosixFilePermission.OTHERS_READ);
        if (f.exists()) {
            Files.setPosixFilePermissions(f.toPath(), set);
        }
    }
}
