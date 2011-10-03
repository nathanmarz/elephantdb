package elephantdb.store;

import elephantdb.DomainSpec;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.List;
import org.apache.hadoop.fs.FileUtil;


public class DomainStore {
   VersionedStore _vs;
   DomainSpec _spec;

   public DomainStore(FileSystem fs, String path) throws IOException {
       this(new VersionedStore(fs, path), null);
   }

   public DomainStore(FileSystem fs, String path, DomainSpec spec) throws IOException {
       this(new VersionedStore(fs, path), spec);
   }

    public DomainStore(String path) throws IOException {
        this(path, null);
    }

    public DomainStore(String path, DomainSpec spec) throws IOException {
       this(new VersionedStore(path), spec);
   }

    protected DomainStore(VersionedStore vs, DomainSpec spec) throws IOException {
        _vs = vs;
       String path = vs.getRoot();
       FileSystem fs = vs.getFileSystem();
       if(DomainSpec.exists(fs, path)) {
           _spec = DomainSpec.readFromFileSystem(fs, path);
           
           if(spec!=null && !_spec.equals(spec)) {
               throw new IllegalArgumentException(spec.toString() + " does not match existing " + _spec.toString());
           }
       } else {
           if(spec == null) {
               throw new IllegalArgumentException("You must supply a DomainSpec when creating a DomainStore.");
           } else {
               _spec = spec;
               spec.writeToFileSystem(fs, path);
           }
       }
   }

   public DomainSpec getSpec() {
       return _spec;
   }

   public FileSystem getFileSystem() {
       return _vs.getFileSystem();
   }

   public String getRoot() {
       return _vs.getRoot();
   }

   public String versionPath(long version) {
       return _vs.versionPath(version);
   }

   public String mostRecentVersionPath() throws IOException {
       return _vs.mostRecentVersionPath();
   }

   public String mostRecentVersionPath(long maxVersion) throws IOException {
       return _vs.mostRecentVersionPath(maxVersion);
   }

   public Long mostRecentVersion() throws IOException {
       return _vs.mostRecentVersion();
   }

   public Long mostRecentVersion(long maxVersion) throws IOException {
       return _vs.mostRecentVersion(maxVersion);
   }

   public String createVersion() throws IOException {
       return _vs.createVersion();
   }

   public String createVersion(long version) throws IOException {
       return _vs.createVersion(version);
   }

   public void failVersion(String path) throws IOException {
       _vs.failVersion(path);
   }

   public void deleteVersion(long version) throws IOException {
       _vs.deleteVersion(version);
   }

   public void succeedVersion(String path) throws IOException {
       _vs.succeedVersion(path);
   }

   public static void synchronizeVersions(FileSystem fs, DomainSpec spec, String oldv, String newv) throws IOException {
       // TODO: might want to do a distcp here if the files are large
       // kept simple here for now since large domains implies large updates which will hit every shard
       if(oldv!=null) {
           for(int i=0; i<spec.getNumShards(); i++) {
               String currPath = oldv + "/" + i;
               String newPath = newv + "/" + i;
               if(fs.exists(new Path(currPath)) && !fs.exists(new Path(newPath))) {
                   if(!FileUtil.copy(fs, new Path(currPath), fs, new Path(newPath), false, false, new Configuration())) {
                       throw new IOException("Unable to synchronize versions");
                   }
               }
           }
       }
   }

   /**
    * When updating the prior version:
    *
    * After the version at path has been completed, call this method to copy over any shards that didn't have any records for them
    * and so weren't processed in the MapReduce job.
    */
   public void synchronizeInProgressVersion(String path) throws IOException {
       synchronizeVersions(getFileSystem(), getSpec(), mostRecentVersionPath(), path);
   }

   public void cleanup() throws IOException {
       _vs.cleanup();
   }

   public void cleanup(int versionsToKeep) throws IOException {
       _vs.cleanup(versionsToKeep);
   }

   public List<Long> getAllVersions() throws IOException {
       return _vs.getAllVersions();
   }

   public boolean hasVersion(long version) throws IOException {
       return _vs.hasVersion(version);
   }
}
