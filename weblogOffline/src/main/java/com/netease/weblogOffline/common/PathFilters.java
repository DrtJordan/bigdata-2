
package com.netease.weblogOffline.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Supplies some useful and repeatedly-used instances of {@link PathFilter}.
 */
public final class PathFilters {

  private static final PathFilter PART_FILE_INSTANCE = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return name.startsWith("part-") && !name.endsWith(".crc");
    }
  };
  
  private PathFilters() {
  }

  /**
   * @return {@link PathFilter} that accepts paths whose file name starts with "part-". Excludes
   * ".crc" files.
   */
  public static PathFilter partFilter() {
    return PART_FILE_INSTANCE;
  }
  

}
