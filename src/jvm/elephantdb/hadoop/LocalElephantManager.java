package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.persistence.Coordinator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class LocalElephantManager {
    public static final String TMP_DIRS_CONF = "elephantdb.local.tmp.dirs";

    public static void setTmpDirs(Configuration conf, List<String> dirs) {
        conf.setStrings(TMP_DIRS_CONF, dirs.toArray(new String[dirs.size()]));
    }

    public static List<String> getTmpDirs(Configuration conf) {
        String[] res = conf.getStrings(TMP_DIRS_CONF, new String[0]);
        List<String> ret = new ArrayList<String>();
        if (res.length == 0) {
            ret.add("/tmp");
        } else {
            Collections.addAll(ret, res);
        }
        return ret;
    }

    FileSystem fs;
    File dirFlag;
    String localRoot;
    DomainSpec spec;
    Map<String, Object> persistenceOptions;

    public LocalElephantManager(FileSystem fs, DomainSpec spec,
        Map<String, Object> persistenceOptions, List<String> tmpDirs) throws IOException {
        localRoot = selectAndFlagRoot(tmpDirs);
        this.fs = fs;
        this.spec = spec;
        this.persistenceOptions = persistenceOptions;
    }

    /**
     * Creates a temporary directory, downloads the remotePath (tied to the FS), and returns it. If
     * remotePath is null or doesn't exist, creates an empty local elephant and closes it.
     * @param id
     * @param remotePath
     * @return
     * @throws java.io.IOException
     */
    public String downloadRemoteShard(String id, String remotePath) throws IOException {
        Coordinator coord = spec.getCoordinator();
        String returnDir = localTmpDir(id);
        if (remotePath == null || !fs.exists(new Path(remotePath))) {
            coord.createPersistence(returnDir, persistenceOptions).close();
        } else {
            fs.copyToLocalFile(new Path(remotePath), new Path(returnDir));
            Collection<File> crcs =
                    FileUtils.listFiles(new File(returnDir), new String[]{"crc"}, true);
            for (File crc : crcs) {
                FileUtils.forceDelete(crc);
            }
        }
        return returnDir;
    }

    public String localTmpDir(String id) {
        return localRoot + "/" + id;
    }

    public void progress() {
        dirFlag.setLastModified(System.currentTimeMillis());
    }


    public void cleanup() throws IOException {
        FileSystem.getLocal(new Configuration()).delete(new Path(localRoot), true);
        dirFlag.delete();
    }

    private void clearStaleFlags(List<String> tmpDirs) {
        //delete any flags more than an hour old
        for (String tmp : tmpDirs) {
            File flagDir = new File(flagDir(tmp));
            flagDir.mkdirs();
            for (File flag : flagDir.listFiles()) {
                if (flag.lastModified() < System.currentTimeMillis() - 1000 * 60 * 60) {
                    flag.delete();
                }
            }
        }
    }

    private String flagDir(String tmpDir) {
        return tmpDir + "/flags";
    }

    private String selectAndFlagRoot(List<String> tmpDirs) throws IOException {
        clearStaleFlags(tmpDirs);
        Map<String, Integer> flagCounts = new HashMap<String, Integer>();
        for (String tmp : tmpDirs) {
            File flagDir = new File(flagDir(tmp));
            flagDir.mkdirs();
            flagCounts.put(tmp, flagDir.list().length);
        }
        String best = null;
        Integer bestCount = null;
        for (Entry<String, Integer> e : flagCounts.entrySet()) {
            if (best == null || e.getValue() < bestCount) {
                best = e.getKey();
                bestCount = e.getValue();
            }
        }
        String token = UUID.randomUUID().toString();
        dirFlag = new File(flagDir(best) + "/" + token);
        dirFlag.createNewFile();
        new File(best).mkdirs();
        return best + "/" + token;
    }
}
