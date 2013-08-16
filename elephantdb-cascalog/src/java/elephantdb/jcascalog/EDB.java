package elephantdb.jcascalog;

import elephantdb.cascading.ElephantDBTap;
import elephantdb.DomainSpec;
import cascalog.Util;
import clojure.lang.*;

public class EDB {
  public static Object makeKeyValTap(String path) {
    return makeKeyValTap(path, null);
  }

  public static Object makeKeyValTap(String path, DomainSpec spec) {
    return makeKeyValTap(path, spec, new ElephantDBTap.Args());
  }

  public static Object makeKeyValTap(String path, DomainSpec spec, ElephantDBTap.Args args) {
    if(args==null) args = new ElephantDBTap.Args();
    IFn keyvalfn = Util.bootSimpleFn("elephantdb.cascalog.keyval", "keyval-tap");
    try {
      return keyvalfn.invoke(path,
                             kw("spec"), spec,
                             kw("tmp-dirs"), args.tmpDirs,
                             kw("source-fields"), args.sourceFields,
                             kw("version"), args.version,
                             kw("timeout-ms"), args.timeoutMs
                             );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Keyword kw(String kw) {
    return Keyword.intern(kw);
  }
}
