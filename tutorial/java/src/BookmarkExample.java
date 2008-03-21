//Our generated classes
import tutorial.*;
import thrudex.*;
import thrudoc.*;

import com.facebook.thrift.TException;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TIOStreamTransport;
import com.facebook.thrift.transport.TSocket; 
import com.facebook.thrift.transport.TFramedTransport;
import com.facebook.thrift.transport.TTransportException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;

import java.io.*;

public class BookmarkExample {

    static final int THRUDEX_PORT = 11299;
    static final int THRUDOC_PORT  = 11291;

    static final String THRUDOC_BUCKET = "bookmarks";    

    static final String THRUDEX_INDEX  = "bookmarks";
    
    private Thrudoc.Client   thrudoc;
    private Thrudex.Client  thrudex;
    
    private ByteArrayOutputStream        mbuf_out;
    private PushbackInputStream mbuf_in;
    private TTransport          mbuf_transport;
    private TBinaryProtocol     mbuf_protocol;

    public BookmarkExample() throws IOException, TException{

        this.connectToThrudoc();
        this.connectToThrudex();

        //Creates a buffer to serialize/deserialize with
        InputStream in = new ByteArrayInputStream(new byte[1]);
        mbuf_in        = new PushbackInputStream(in,10000); //supports 10k msg size
        mbuf_out       = new ByteArrayOutputStream();
        mbuf_transport = new TIOStreamTransport(mbuf_in, mbuf_out); 
        mbuf_protocol  = new TBinaryProtocol(mbuf_transport); 
    }

    private void connectToThrudoc() throws TException {
        TSocket          socket    = new TSocket("localhost", THRUDOC_PORT );
        TFramedTransport transport = new TFramedTransport(socket);
        TBinaryProtocol  protocol  = new TBinaryProtocol(transport);
         
        thrudoc = new Thrudoc.Client(protocol);

        transport.open();
    }

    private void connectToThrudex() throws TException {
        TSocket          socket    = new TSocket("localhost", THRUDEX_PORT );
        TFramedTransport transport = new TFramedTransport(socket);
        TBinaryProtocol  protocol  = new TBinaryProtocol(transport);
         
        thrudex = new Thrudex.Client(protocol);

        transport.open();
    }

    private String serialize(Bookmark b) throws TException, IOException{

        b.write(mbuf_protocol);
        mbuf_out.flush();
        
        String b_str = mbuf_out.toString();

        mbuf_out.reset();

        return b_str;
    }

    private Bookmark deserialize( String b_str ) throws TException, IOException {
        
        mbuf_in.unread( b_str.getBytes() );        

        Bookmark b = new Bookmark();
        b.read(mbuf_protocol);


        return b;
    }

    public void loadTSVFile(String file) throws IOException, TException, ThrudocException, ThrudexException{

        long t0 = System.currentTimeMillis();

        FileReader input   = new FileReader(file);
        BufferedReader buf = new BufferedReader(input);
        
        String line;
  
        while( (line = buf.readLine()) != null ){
            String [] arr = line.split("\t");

            Bookmark b = new Bookmark();
            b.url      = arr[0];
            b.title    = arr[1];
            b.tags     = arr[2];

            this.addBookmark(b);           
        }

        // thrudex.commitAll();
        
        input.close();

        long t1 = System.currentTimeMillis();
       

        System.out.println("*Indexed file in: "+(t1-t0)+"ms*\n");
    }

    private void addBookmark( Bookmark b ) throws IOException, TException, ThrudocException, ThrudexException{
        
        String id = this.storeBookmark(b);
        
        this.indexBookmark(id,b);
    }

    public Bookmark getBookmark( String id ) throws TException, ThrudocException, IOException{

        String b_str = thrudoc.get(THRUDOC_BUCKET,id);

        return this.deserialize(b_str);
    }
    
        
    private String storeBookmark( Bookmark b ) throws TException, ThrudocException, IOException{

        String b_str = this.serialize(b);
        String id    = thrudoc.putValue(THRUDOC_BUCKET,b_str);

        return id;
    }
    
    private void indexBookmark( String id, Bookmark b ) throws TException, ThrudexException{
         
        Document doc = new Document();
        
        doc.key  = id;
        doc.index  = THRUDEX_INDEX;
        doc.fields = new ArrayList<Field>();

        Field field = new Field();

        //title
        field.key     = "title";
        field.value    = b.title;
        field.sortable = true;
        doc.fields.add(field);
    
        //tags
        field = new Field();
        field.key  = "tags";
        field.value = b.tags;
        doc.fields.add(field);

        thrudex.put( doc );
    }
     
    public void removeAll() throws TException, ThrudocException, ThrudexException {
       
        long t0 = System.currentTimeMillis();

        //chunks of 100
        int limit  = 100;
        String seed = new String();

        ScanResponse r;

        do{
            r = thrudoc.scan(THRUDOC_BUCKET,seed,limit);

            if(r.elements.size() == 0)
                break;

            ArrayList<thrudex.Element> docs = new ArrayList<thrudex.Element>();

            for( thrudoc.Element e : r.elements ){
                thrudex.Element rm = new thrudex.Element();
                rm.index  = THRUDEX_INDEX;
                rm.key  = e.key;
                
                docs.add(rm);
            }

            thrudex.removeList(docs);
            thrudoc.removeList(r.elements);

            seed = r.seed;


        }while(r.elements.size() == limit);

        
        long t1 = System.currentTimeMillis();

        System.out.println("\n*Index cleared in: "+(t1-t0)+"ms*");        
    }

    public void find(String terms, HashMap<String,String> options) throws IOException, TException, ThrudocException, ThrudexException{

        System.out.print("Searching for: '"+terms+"' ");
        for (String key : options.keySet()) {
            System.out.print(key+":"+options.get(key)+" ");
        }
        System.out.print("\n");

        long t0 = System.currentTimeMillis();
        
        SearchQuery query  = new SearchQuery();
         
        query.index     = THRUDEX_INDEX;
        query.query     = terms;

        query.limit     =100;
        //query.offset  =10;

        if( options.get("random") != null )
            query.randomize = true;

        if( options.get("sortby") != null )            
            query.sortby = options.get("sortby");

        SearchResponse ids = thrudex.search( query );

        System.out.println("Found "+ids.total+" bookmarks");
        
        if( ids.elements.size() > 0 ) {

            List<ListResponse> response = thrudoc.getList( createDocList(ids.elements) );            

            ArrayList<Bookmark> bookmarks = new ArrayList<Bookmark>();
            
            for(ListResponse lr : response ){
                if(lr.element.value != "")
                    bookmarks.add( this.deserialize( lr.element.value ) );
                else
                    System.out.println("Error fetching document");
            }

            this.printBookmarks(bookmarks);
        }

        long t1 = System.currentTimeMillis();
        System.out.println("Took: "+(t1-t0)+"ms\n");  
    }

    private List<thrudoc.Element> createDocList( List<thrudex.Element> ids ) throws ThrudocException {
        
        ArrayList<thrudoc.Element> docs = new ArrayList<thrudoc.Element>();

        for( thrudex.Element element : ids ){
            thrudoc.Element el = new thrudoc.Element();
            el.key    = element.key;
            el.bucket = THRUDOC_BUCKET;

            docs.add(el);
        }

        return docs;
    }

    private void printBookmarks( ArrayList<Bookmark> bookmarks ){
        
        int i = 1;
        for(Bookmark b : bookmarks) {
            System.out.println(i+"\ttitle :\t"+b.title);
            System.out.println("\turl   :\t("+b.url+")");
            System.out.println("\ttags  :\t("+b.tags+")");
            i++;
        }
    }
    
    
    public static void main(String [] args) {
        try {
            
            BookmarkExample bm = new BookmarkExample();

            bm.loadTSVFile("../bookmarks.tsv");

            HashMap<String,String> o1 = new HashMap<String,String>();
            o1.put("random","true");

            bm.find("tags:(+css +examples)", o1 );


            HashMap<String,String> o2 = new HashMap<String,String>();
            o2.put("sortby", "title");
            
            bm.find("title:(linux)",o2);

            bm.removeAll();

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
