package org.apache.lucene.index;

import java.io.IOException;
import java.nio.file.FileSystems;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * @author pfliu
 * @date 2021/01/03.
 * @brief 测试testInwriter
 */
public class HuYanTestIndexWriter extends LuceneTestCase {

  public void testAddDocument() throws IOException {

    Directory d = FSDirectory.open(FileSystems.getDefault().getPath(
        "/tmp/lucene-test"));

    // 索引写入的配置
    Analyzer analyzer = new StandardAnalyzer();// 分词器
    IndexWriterConfig conf = new IndexWriterConfig(analyzer);
    conf.setUseCompoundFile(false);

    // 构建用于操作索引的类
    IndexWriter indexWriter = new IndexWriter(d, conf);

    // add one document & close writer
//    for (int i = 0; i < 3000; i++) {
    addDoc(indexWriter, 0);
//
//    }
    indexWriter.commit();
    indexWriter.close();
    d.close();
  }

  static void addDoc(IndexWriter writer, int i) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa" + i, Field.Store.YES));
    writer.addDocument(doc);
  }
}
