package elephantdb.future;

import org.apache.lucene.document.Document;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 11:50 PM
 */
public class LuceneDocument implements elephantdb.persistence.Document {
    Document document;

    public LuceneDocument() {
    }

    public LuceneDocument(Document luceneDoc) {
        this.document = luceneDoc;
    }

}
