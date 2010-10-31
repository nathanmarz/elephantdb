package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.persistence.LocalPersistenceFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class LocalElephantManager {
        public static final String TMP_DIRS_CONF = "elephantdb.local.tmp.dirs";

        public static void setTmpDirs(Configuration conf, List<String> dirs) {
            conf.setStrings(TMP_DIRS_CONF, dirs.toArray(new String[dirs.size()]));
        }

        public static List<String> getTmpDirs(Configuration conf) {
            String[] res = conf.getStrings(TMP_DIRS_CONF, new String[0]);
            List<String> ret =  new ArrayList<String>();
            if(res.length==0) {
                ret.add("/tmp");
            } else {
                for(String s: res) {
                    ret.add(s);
                }
            }
            return ret;
        }

        FileSystem _fs;
        File _dirFlag;
        String _localRoot;
        DomainSpec _spec;
        Map<String, Object> _persistenceOptions;
        
        public LocalElephantManager(FileSystem fs, DomainSpec spec, Map<String, Object> persistenceOptions, List<String> tmpDirs) throws IOException {
            _localRoot = selectAndFlagRoot(tmpDirs);
            _fs = fs;
            _spec = spec;
            _persistenceOptions = persistenceOptions;
        }

        /**
         * Creates a temporary directory, downloads the remotePath (tied to the FS), and returns it. If remotePath is null or doesn't exist,
         * creates an empty local elephant and closes it.
         */
        public String downloadRemoteShard(String id, String remotePath) throws IOException {
            LocalPersistenceFactory fact = _spec.getLPFactory();
            String returnDir = localTmpDir(id);
            if(remotePath==null || !_fs.exists(new Path(remotePath))) {
                fact.createPersistence(
                            returnDir,
                            _persistenceOptions)
                         .close();
            } else {
                if(_fs instanceof ChecksumFileSystem) {
                   ((ChecksumFileSystem)_fs).copyToLocalFile(new Path(remotePath), new Path(returnDir), false);
                } else {
                    _fs.copyToLocalFile(new Path(remotePath), new Path(returnDir));
                }
            }
            return returnDir;
        }

        public String localTmpDir(String id) {
            return _localRoot + "/" + id;
        }

        public void progress() {
            _dirFlag.setLastModified(System.currentTimeMillis());
        }


        public void cleanup() throws IOException {
            FileSystem.getLocal(new Configuration()).delete(new Path(_localRoot), true);
            _dirFlag.delete();
        }

        private void clearStaleFlags(List<String> tmpDirs) {
            //delete any flags more than an hour old
            for(String tmp: tmpDirs) {
                File flagDir = new File(flagDir(tmp));
                flagDir.mkdirs();
                for(File flag: flagDir.listFiles()) {
                    if(flag.lastModified() < System.currentTimeMillis() - 1000*60*60) {
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
            for(String tmp: tmpDirs) {
                File flagDir = new File(flagDir(tmp));
                flagDir.mkdirs();
                flagCounts.put(tmp, flagDir.list().length);
            }
            String best = null;
            Integer bestCount = null;
            for(Entry<String, Integer> e: flagCounts.entrySet()) {
                if(best==null || e.getValue() < bestCount) {
                    best = e.getKey();
                    bestCount = e.getValue();
                }
            }
            String token = UUID.randomUUID().toString();
            _dirFlag = new File(flagDir(best) + "/" + token);
            _dirFlag.createNewFile();
            new File(best).mkdirs();
            return best + "/" + token;
        }
}
