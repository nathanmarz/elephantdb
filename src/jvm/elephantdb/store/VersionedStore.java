package elephantdb.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class VersionedStore {
    private static final String FINISHED_VERSION_SUFFIX = ".version";

    private String _root;
    private FileSystem _fs;

    public VersionedStore(String path) throws IOException {
      this(getFS(path), path);
    }
    
    public VersionedStore(FileSystem fs, String path) throws IOException {
      _fs = fs;
      _root = path;
      _fs.mkdirs(new Path(_root));
    }

    public FileSystem getFileSystem() {
        return _fs;
    }

    public String getRoot() {
        return _root;
    }

    public String versionPath(long version) {
        return new Path(_root, "" + version).toString();
    }

    public String mostRecentVersionPath() throws IOException {
        return versionPath(mostRecentVersion());
    }

    public String mostRecentVersionPath(long maxVersion) throws IOException {
        return versionPath(mostRecentVersion(maxVersion));
    }

    public long mostRecentVersion() throws IOException {
        List<Long> all = getAllVersions();
        if(all.size()==0) throw new RuntimeException("Versioned store is empty!");
        return all.get(0);
    }

    public long mostRecentVersion(long maxVersion) throws IOException {
        List<Long> all = getAllVersions();
        for(Long v: all) {
            if(v <= maxVersion) return v;
        }
        throw new RuntimeException("Could not find a version older than " + maxVersion);
    }

    public String createVersion() throws IOException {
        return createVersion(System.currentTimeMillis());
    }

    public String createVersion(long version) throws IOException {
        String ret = versionPath(version);
        if(_fs.exists(new Path(ret)) || getAllVersions().contains(version))
            throw new RuntimeException("Version already exists or data already exists");
        else
            return ret;
    }

    public void failVersion(String path) throws IOException {
        deleteVersion(validateAndGetVersion(path));
    }

    public void deleteVersion(long version) throws IOException {
        _fs.delete(new Path(versionPath(version)), true);
        _fs.delete(new Path(tokenPath(version)), false);
    }

    public void succeedVersion(String path) throws IOException {
        long version = validateAndGetVersion(path);
        _fs.createNewFile(new Path(tokenPath(version)));
    }

    public void cleanup() throws IOException {
        cleanup(-1);
    }

    public void cleanup(int versionsToKeep) throws IOException {
        List<Long> versions = getAllVersions();
        if(versionsToKeep >= 0) {
            versions = versions.subList(0, Math.min(versions.size(), versionsToKeep));
        }
        HashSet<Long> keepers = new HashSet<Long>(versions);

        for(FileStatus s: _fs.listStatus(new Path(_root))) {
            Path p = s.getPath();
            Long v = parseVersion(p.toString());
            if(v==null || !keepers.contains(v)) {
                _fs.delete(p, true);
            }
        }
    }

    /**
     * Sorted from most recent to oldest
     */
    public List<Long> getAllVersions() throws IOException {
        List<Long> ret = new ArrayList<Long>();
        for(FileStatus s: _fs.listStatus(new Path(_root))) {
            if(!s.isDir() && s.getPath().getName().endsWith(FINISHED_VERSION_SUFFIX)) {
                ret.add(validateAndGetVersion(s.getPath().toString()));
            }
        }
        Collections.sort(ret);
        Collections.reverse(ret);
        return ret;
    }

    private String tokenPath(long version) {
        return new Path(_root, "" + version + FINISHED_VERSION_SUFFIX).toString();
    }

    private Path normalizePath(String p) {
        return new Path(p).makeQualified(_fs);
    }

    private long validateAndGetVersion(String path) {
        if(!normalizePath(path).getParent().equals(normalizePath(_root))) {
            throw new RuntimeException(path + " " + new Path(path).getParent() + " is not part of the versioned store located at " + _root);
        }
        Long v = parseVersion(path);
        if(v==null) throw new RuntimeException(path + " is not a valid version");
        return v;
    }

    private Long parseVersion(String path) {
        String name = new Path(path).getName();
        if(name.endsWith(FINISHED_VERSION_SUFFIX)) {
            name = name.substring(0, name.length()-FINISHED_VERSION_SUFFIX.length());
        }
        try {
            return Long.parseLong(name);
        } catch(NumberFormatException e) {
            return null;
        }
    }

    protected static FileSystem getFS(String path) throws IOException {
        return new Path(path).getFileSystem(new Configuration());
    }
}
