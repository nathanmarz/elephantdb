package elephantdb.store;

import elephantdb.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class VersionedStore {
    private static final String FINISHED_VERSION_SUFFIX = ".version";

    private String root;
    private FileSystem fs;

    public VersionedStore(String path) throws IOException {
        this(Utils.getFS(path, new Configuration()), path);
    }

    public VersionedStore(FileSystem fs, String path) throws IOException {
        this.fs = fs;

        try {
            root = new URI(path).getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        mkdirs(root);
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    public String getRoot() {
        return root;
    }

    public String versionPath(long version) {
        return new Path(root, "" + version).toString();
    }

    public String mostRecentVersionPath() throws IOException {
        Long v = mostRecentVersion();
        return (v == null) ? null : versionPath(v);
    }

    public String mostRecentVersionPath(long maxVersion) throws IOException {
        Long v = mostRecentVersion(maxVersion);
        return (v == null) ? null : versionPath(v);
    }

    public Long mostRecentVersion() throws IOException {
        List<Long> all = getAllVersions();
        return (all.size()==0) ? null : all.get(0);
    }

    public Long mostRecentVersion(long maxVersion) throws IOException {
        List<Long> all = getAllVersions();
        for(Long v: all) {
            if(v <= maxVersion)
                return v;
        }
        return null;
    }

    public String createVersion() throws IOException {
        return createVersion(System.currentTimeMillis());
    }

    public String createVersion(long version) throws IOException {
        String ret = versionPath(version);
        if(getAllVersions().contains(version))
            throw new RuntimeException("Version already exists or data already exists");
        else {
            //in case there's an incomplete version there, delete it
            fs.delete(new Path(versionPath(version)), true);
            return ret;
        }
    }

    public void failVersion(String path) throws IOException {
        deleteVersion(validateAndGetVersion(path));
    }

    public void deleteVersion(long version) throws IOException {
        fs.delete(new Path(versionPath(version)), true);
        fs.delete(new Path(tokenPath(version)), false);
    }

    public void succeedVersion(String path) throws IOException {
        succeedVersion(validateAndGetVersion(path));
    }

    public void succeedVersion(long version) throws IOException {
        createNewFile(tokenPath(version));
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

        for(Path p: listDir(root)) {
            Long v = parseVersion(p.toString());
            if(v!=null && !keepers.contains(v)) {
                fs.delete(p, true);
            }
        }
    }

    /**
     * Sorted from most recent to oldest
     */
    public List<Long> getAllVersions() throws IOException {
        List<Long> ret = new ArrayList<Long>();

        Path rootPath = new Path(getRoot());
        if (getFileSystem().exists(rootPath)) {
            for(Path p: listDir(root)) {
                if(p.getName().endsWith(FINISHED_VERSION_SUFFIX)) {
                    ret.add(validateAndGetVersion(p.toString()));
                }
            }
            Collections.sort(ret);
            Collections.reverse(ret);
        }
        return ret;
    }

    public boolean hasVersion(long version) throws IOException {
        return getAllVersions().contains(version);
    }

    private String tokenPath(long version) {
        return new Path(root, "" + version + FINISHED_VERSION_SUFFIX).toString();
    }

    private Path normalizePath(String p) {
        return new Path(p).makeQualified(fs);
    }

    private long validateAndGetVersion(String path) {
        if(!normalizePath(path).getParent().equals(normalizePath(root))) {
            throw new RuntimeException(path + " " + new Path(path).getParent() + " is not part of the versioned store located at " + root);
        }
        Long v = parseVersion(path);
        if(v==null) throw new RuntimeException(path + " is not a valid version");
        return v;
    }

    public Long parseVersion(String path) {
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

    private void createNewFile(String path) throws IOException {
        if(fs instanceof LocalFileSystem)
            new File(path).createNewFile();
        else
            fs.createNewFile(new Path(path));
    }

    private void mkdirs(String path) throws IOException {
        if(fs instanceof LocalFileSystem)
            new File(path).mkdirs();
        else {
            try {
                fs.mkdirs(new Path(path));
            } catch (AccessControlException e) {
                throw new RuntimeException("Root directory doesn't exist, and user doesn't have the permissions " +
                        "to create" + path + ".", e);
            }
        }
    }


    private List<Path> listDir(String dir) throws IOException {
        List<Path> ret = new ArrayList<Path>();
        if(fs instanceof LocalFileSystem) {
            for(File f: new File(dir).listFiles()) {
                ret.add(new Path(f.getAbsolutePath()));
            }
        } else {
            for(FileStatus status: fs.listStatus(new Path(dir))) {
                ret.add(status.getPath());
            }
        }
        return ret;
    }
}
