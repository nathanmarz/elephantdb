package elephantdb.document;

import org.apache.lucene.document.Document;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 11:50 PM
 */
public class LuceneDocument implements elephantdb.document.Document {
    Document document;

    public LuceneDocument() {
    }

    public LuceneDocument(Document luceneDoc) {
        this.document = luceneDoc;
    }
    
    public Document getDocument() {
        return document;
    }

}
