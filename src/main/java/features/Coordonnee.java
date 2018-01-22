package features;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


public class Coordonnee implements Serializable, Comparable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7833959358739191534L;
	int x, y;
	
	public Coordonnee(int x, int y) {
		this.x = x;
		this.y = y;
	}
	
	public String toString() {
		return "("+x+","+y+")";
	}
	
    private  void readObject(ObjectInputStream ois)
    throws IOException, ClassNotFoundException {

  
       this.x = ois.readInt() ;
       this.y = ois.readInt() ;
 
   }


    private  void writeObject(ObjectOutputStream oos)
    throws IOException {

     
      oos.writeInt(x) ;
      oos.writeInt(y) ;
    
   }


	public int compareTo(Object o) {
		if(this.x != ((Coordonnee)o).x && this.y != ((Coordonnee)o).y)
			return -1;
		return 0;
	} 
}
