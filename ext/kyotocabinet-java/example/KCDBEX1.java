import kyotocabinet.*;

public class KCDBEX1 {
  public static void main(String[] args) {

    // create the object
    DB db = new DB();

    // open the database
    if (!db.open("casket.kch", DB.OWRITER | DB.OCREATE)){
      System.err.println("open error: " + db.error());
    }

    // store records
    if (!db.set("foo", "hop") ||
        !db.set("bar", "step") ||
        !db.set("baz", "jump")){
      System.err.println("set error: " + db.error());
    }

    // retrieve records
    String value = db.get("foo");
    if (value != null){
      System.out.println(value);
    } else {
      System.err.println("set error: " + db.error());
    }

    // traverse records
    Cursor cur = db.cursor();
    cur.jump();
    String[] rec;
    while ((rec = cur.get_str(true)) != null) {
      System.out.println(rec[0] + ":" + rec[1]);
    }
    cur.disable();

    // close the database
    if(!db.close()){
      System.err.println("close error: " + db.error());
    }

  }
}
