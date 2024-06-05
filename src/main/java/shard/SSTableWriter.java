package shard;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

public class SSTableWriter {

  private final static String DATA_FILE_HEADER_FORMAT = "V1:::\n";
  private final static String ROW_ENTRY_FORMAT = "%s_%s\n";
  private final static String INDEX_ENTRY_FORMAT = "%s_%d\n";
  private final static String DATA_FILE_NAME_FORMAT = "%s-%s-data.db";
  private final static String INDEX_FILE_NAME_FORMAT = "%s-%s-index.idx";

  public static void write(int generation, int ssTableGenerationId, Memtable memtable) {
    String generationStrVal = String.valueOf(generation);
    String generationIdStrVal = String.valueOf(ssTableGenerationId);

    Iterator<Map.Entry<String, String>> memtableValues = memtable.getMemtableEntries();
    Path dataFilePath = Paths.get(String.format(DATA_FILE_NAME_FORMAT, generationStrVal, generationIdStrVal));
    Path indexFilePath = Paths.get(String.format(INDEX_FILE_NAME_FORMAT, generationStrVal, generationIdStrVal));
    FileChannel dataFileBufferedWriter;
    FileChannel indexFileBufferedWriter;
    try {
      dataFileBufferedWriter = FileChannel.open(dataFilePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      indexFileBufferedWriter = FileChannel.open(indexFilePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    long currentOffset = 0;
    while (memtableValues.hasNext()) {
      Map.Entry<String, String> rowEntry = memtableValues.next();
      byte[] indexEntryBytes = String.format(INDEX_ENTRY_FORMAT, rowEntry.getKey(), currentOffset).getBytes();
      byte[] dataEntryBytes = String.format(ROW_ENTRY_FORMAT, rowEntry.getKey(), rowEntry.getValue()).getBytes();
      try {
        ByteBuffer buffer = ByteBuffer.wrap(dataEntryBytes);
        dataFileBufferedWriter.write(buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      try {
        ByteBuffer buffer = ByteBuffer.wrap(indexEntryBytes);
        indexFileBufferedWriter.write(buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      currentOffset = currentOffset + dataEntryBytes.length;
    }
    try {
      dataFileBufferedWriter.close();
      indexFileBufferedWriter.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
