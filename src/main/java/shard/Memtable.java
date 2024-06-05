package shard;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class Memtable {

  private final TreeMap<String, String> memtable;
  private long currentMemTableSizeBytes;
  private boolean isLocked;

  public Memtable() {
    memtable = new TreeMap<>();
    isLocked = false;
  }

  public void put(String key, String val) {
    memtable.put(key, val);
    currentMemTableSizeBytes += key.getBytes().length + val.getBytes().length;
  }

  public Optional<String> get(String key) {
    String result = memtable.get(key);
    if (result != null) {
      return Optional.of(result);
    }
    return Optional.empty();
  }

  public void clear() {
    memtable.clear();
    currentMemTableSizeBytes = 0;
  }

  public long getSize() {
    return currentMemTableSizeBytes;
  }

  public Iterator<Map.Entry<String, String>> getMemtableEntries() {
    return memtable.entrySet().iterator();
  }

  public synchronized void lock() {
    isLocked = true;
  }

  public synchronized void unlock() {
    isLocked = false;
  }
}
