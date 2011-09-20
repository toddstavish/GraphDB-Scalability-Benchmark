package bench.utils;


import java.util.List;
import java.util.ArrayList;

/**
 * This class is used as a Data Access Object for people nodes in the Neo4J Graph database.
 */

public class PersonDAO 
{
	
  public String name;
  public int size;
  public List<String> imports = new ArrayList<String>();

  public PersonDAO() {}

}