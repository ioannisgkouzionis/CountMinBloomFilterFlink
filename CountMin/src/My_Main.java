/**
 * 
 */

/**
 * @author igkouzionis
 *
 */
public class My_Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        Count_Min s = new Count_Min(10,10,30);
        s.add(20,1);
        s.add(10,3);
        s.add(30,1);
        s.add(10,1);
        
        Count_Min t = new Count_Min(10,10,30);
        t.add(10, 2);
        t.add("hello", 1);
        
       // t.merge(s);
        
        System.out.println(s.estimateCount(10));
        System.out.println(t.estimateCount(10));
	}

}
