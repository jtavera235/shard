package shard;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class Wal {

  private static final String WAL_PATH = "wal.log";
  private final FileChannel bufferedWriter;

  public Wal() {
    final Path path = Paths.get(WAL_PATH);
    try {
      bufferedWriter = FileChannel.open(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void append(String key, String val) {
  }

  public void clear() {

  }
}
