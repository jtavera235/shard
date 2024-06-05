package shard;

import java.util.Optional;

public class LSMShard {

  private final static long DEFAULT_FLUSH_MEMTABLE_SIZE_BYTES = 1024;
  private final static int BASE_SSTABLE_GENERATION = 0;


  private long flushMemTableLimitBytes;
  private Memtable memtable;
  private Wal wal;
  private int baseGenerationCount;

  public LSMShard() {
    memtable = new Memtable();
    wal = new Wal();
    baseGenerationCount = 0;
  }

  public void put(String key, String val) {
    wal.append(key, val);
    memtable.put(key, val);
    if (memtable.getSize() >= DEFAULT_FLUSH_MEMTABLE_SIZE_BYTES) {
      SSTableWriter.write(BASE_SSTABLE_GENERATION, baseGenerationCount, memtable);
      memtable.clear();
      wal.clear();
      baseGenerationCount += 1;
    }
  }

  public String get(String key) {
    Optional<String> memtableResult = memtable.get(key);
    if (memtableResult.isPresent()) {
      return memtableResult.get();
    }
    return null;
  }
}
