
public class Test1 implements java.io.Serializable
{
   public String name;
   public String address;
   public transient int SSN;
   public int number;
   public void mailCheck()
   {
      System.out.println("Mailing a check to " + name
                           + " " + address);
   }
   public static void main(String[] args) {
	Test1 t= new Test1();
	t.mailCheck();
}
}
