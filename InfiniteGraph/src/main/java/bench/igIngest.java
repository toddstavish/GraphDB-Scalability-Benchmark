package bench;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Stack;
import java.util.Arrays;
import java.util.Random;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Properties;

import java.io.FileReader;
import java.io.IOException;

import com.infinitegraph.EdgeKind;
import com.infinitegraph.AccessMode;
import com.infinitegraph.Transaction;
import com.infinitegraph.GraphFactory;
import com.infinitegraph.VertexHandle;
import com.infinitegraph.GraphDatabase;
import com.infinitegraph.StorageException;
import com.infinitegraph.ConfigurationException;
import com.infinitegraph.navigation.Qualifier;
import com.infinitegraph.navigation.qualifiers.VertexTypes;

import au.com.bytecode.opencsv.CSVReader;

import bench.utils.Stopwatch;
import bench.utils.CsvDataGenerator;

/**
 * This class is used to load the InfiniteGraph database.
 * It requires a configuration.properties file to learn the
 * evironment. Additionally, it outputs the text and CSV files
 * that comprise the data in the graph. Large vm heap sizes are
 * necessary for this to run well.
 * @param PROPERTIES  properties.configuration file
 * @param names.txt   census name list
 */

public class igIngest
{


    /**
     * Holds configuration properties on InfiniteGraph, nodes and edge numbers, paths to CSV,
     * and graph.
	 */

	private static Properties PROPERTIES;

	/**
 	* Runtime map, resolves topic strings to ooIds
 	*/

	private static HashMap<String,Long> TOPICMAP;

	/**
	* Runtime maps, resolves group strings to ooIds
	*/

	private static HashMap<String,Long> GROUPMAP;

	/**
 	* Runtime maps, resolves person strings ooIds
 	*/

	private static HashMap<String,Long> PERSONMAP;

	/**
 	* Main method is used to load the InfiniteGraph database.
 	*/

	public static void main(String[] args) throws IOException, ClassNotFoundException
	{

		// Load configuration properties
		PROPERTIES = new Properties();
		PROPERTIES.load(new FileReader("configuration.properties"));

		// Print node space dimensions
		System.out.println("Node space dimensions:");
		System.out.println("Number of people = " + PROPERTIES.getProperty("NUMBER_OF_PEOPLE"));
		System.out.println("Number of topics = " + PROPERTIES.getProperty("NUMBER_OF_TOPICS"));
		System.out.println("Number of groups = " + PROPERTIES.getProperty("NUMBER_OF_GROUPS"));
		System.out.println("Number of documents = " + PROPERTIES.getProperty("NUMBER_OF_DOCUMENTS"));
		System.out.println("Number of topics per person = " + PROPERTIES.getProperty("NUMBER_OF_TOPICS_PER_PERSON"));

		// Generate synthetic CSV data
		//generateCsvData();

		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		// Build graph
		loadDataToGraph();

		// Stop timer
	    timer.stop();
	    System.out.println("Ingested graph in: "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Graph size: " + " nodes.");
	}

	/**
	 * Method to load the various node types into the graph database
	 * Order depenency topics, people, then documents.
	 * Creates and uses CSV and text files.
	 */

	private static void loadDataToGraph() throws IOException
    {
    	
        // Create null transaction, null graph database instance
        Transaction tx = null;
        GraphDatabase graphDB = null;

        // Name for graph database and property file
        String graphDbName = "igIngest";
        String propertiesFileName = "configuration.properties";

        try
        {
        	try
            {
        		// Delete graph database if it already exists
        		GraphFactory.delete(graphDbName, propertiesFileName);
        	}
        	catch (StorageException sE)
        	{
        		System.out.println(sE.getMessage());
        	}

        	// HINT: Add code to create graph database and its contents
        	// Create graph database
    		System.out.println("> Creating graph database ...");
    		GraphFactory.create(graphDbName, propertiesFileName);

    		// Open graph database
    		System.out.println("> Opening graph database ...");
    		graphDB = GraphFactory.open(graphDbName, propertiesFileName);

    		// Begin transaction
    		System.out.println("> Starting a read/write transaction ...");
    		tx = graphDB.beginTransaction(AccessMode.READ_WRITE);
            
            // Create root node
            Root root = new Root();
            graphDB.addVertex(root);
            graphDB.nameVertex("root", root);
			
    		// Create graph
    		System.out.println("> Creating topic nodes ...");
            createTopicNodes(graphDB);
            System.out.println("> Creating group nodes ...");
            createGroupNodes(graphDB);
            System.out.println("> Creating people nodes ...");
            createPeopleNodes(graphDB);
            System.out.println("> Creating document nodes ...");
            createDocumentNodes(graphDB);
        
    		// Commit to save your changes to the graph database
    		System.out.println("> Committing changes ...");
    		tx.commit();
        }
        catch (ConfigurationException cE)
        {
            System.out.println("> Configuration Exception was thrown ... ");
            System.out.println(cE.getMessage());
        }

        finally
        {
            // If the transaction was not committed, complete
            // will roll it back
            if (tx != null)
                tx.complete();
            if (graphDB != null)
            {
                graphDB.close();
                System.out.println("> On Exit: Closed graph database");
            }
        }  
    }
    

	/**
	 * Method to create topic nodes in the graph database
	 */

	private static void createTopicNodes(GraphDatabase graphDB) throws IOException
    {
		String topic;
		Root root = (Root)graphDB.getNamedVertex("root");
		Topics topicsNode = new Topics();
		graphDB.addVertex(topicsNode);
		TopicsEdge topicsEdge = new TopicsEdge();
		root.addEdge(topicsEdge, topicsNode, EdgeKind.BIDIRECTIONAL);				
		ArrayList<String> topics = CsvDataGenerator.load(PROPERTIES.getProperty("TOPICS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS")));
		Iterator<String> topicsItr = topics.iterator();
		TOPICMAP = new HashMap<String,Long>();
		while (topicsItr.hasNext())
		{
			topic = topicsItr.next();
			Topic topicNode = new Topic(topic);
		    Long topicId = graphDB.addVertex(topicNode);
			TOPICMAP.put(topic, topicId);
			TopicEdge topicEdge = new TopicEdge();
			topicsNode.addEdge(topicEdge, topicNode, EdgeKind.BIDIRECTIONAL);		
	    }
	}

	/**
	 * Method to create group nodes in the graph database
	 */

	private static void createGroupNodes(GraphDatabase graphDB) throws IOException
    {
		String group;
		Root root = (Root)graphDB.getNamedVertex("root");
		Groups groupsNode = new Groups();
		graphDB.addVertex(groupsNode);
		GroupsEdge groupsEdge = new GroupsEdge();
		root.addEdge(groupsEdge, groupsNode, EdgeKind.BIDIRECTIONAL);						
		ArrayList<String> groups = CsvDataGenerator.load(PROPERTIES.getProperty("GROUPS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_GROUPS")));
		Iterator<String> groupsItr = groups.iterator();
		GROUPMAP = new HashMap<String,Long>();
		while (groupsItr.hasNext())
		{
			group = groupsItr.next();
			Group groupNode = new Group(group);
			Long groupId = graphDB.addVertex(groupNode);
			GROUPMAP.put(group, groupId);
			GroupEdge groupEdge = new GroupEdge();
			groupsNode.addEdge(groupEdge, groupNode, EdgeKind.BIDIRECTIONAL);
	    }
	}

	/**
	 * Method to create person nodes in the graph database
	 */

	private static void createPeopleNodes(GraphDatabase graphDB) throws IOException
    {
		// Create people nodes and attach them to root, put them into two groups randomly
	  	String name, group;
		Random random = new Random();
		Root root = (Root)graphDB.getNamedVertex("root");
		People peopleNode = new People();
		graphDB.addVertex(peopleNode);
		PeopleEdge peopleEdge = new PeopleEdge();
		root.addEdge(peopleEdge, peopleNode, EdgeKind.BIDIRECTIONAL);
		ArrayList<String> names = CsvDataGenerator.load(PROPERTIES.getProperty("NAMES_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_PEOPLE")));
		ArrayList<String> groups = CsvDataGenerator.load(PROPERTIES.getProperty("GROUPS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_GROUPS")));
		ArrayList<String> topics = CsvDataGenerator.load(PROPERTIES.getProperty("TOPICS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS")));
		Iterator<String> namesItr = names.iterator();
		PERSONMAP = new HashMap<String,Long>();
	 	while (namesItr.hasNext())
		{
			name = namesItr.next();
			Person personNode = new Person(name);
			long personNodeId = graphDB.addVertex(personNode);
			PERSONMAP.put(name, personNodeId);
			PersonEdge personEdge = new PersonEdge();
			peopleNode.addEdge(personEdge, personNode, EdgeKind.BIDIRECTIONAL);	
			group = groups.get(random.nextInt(groups.size()));
			long groupNodeId = GROUPMAP.get(group);
			IsMemberOf memberOf = new IsMemberOf();
			graphDB.addEdge(memberOf, personNodeId, groupNodeId, EdgeKind.BIDIRECTIONAL, (short)0);
	    }

		// Associate people and topics to people
		String[] row;
		CSVReader csvReader = new CSVReader(new FileReader(PROPERTIES.getProperty("PEOPLE_PATH")));
		while ((row = csvReader.readNext()) != null)
		{
			int column = 0;
			long personNodeId = 0;
			int topicWeight = 1;
			long topicNodeId = 0;
			long friendNodeId = 0;
			for (String cell : row)
			{
				// Who the person knows
				if (column == 0)
				{
					personNodeId = PERSONMAP.get(row[column]);
					column++;
				}
				else if (column <= Integer.decode(PROPERTIES.getProperty("NUMBER_OF_PEOPLE_PER_PERSON")))
				{
					friendNodeId = PERSONMAP.get(row[column]);
					Knows knows = new Knows(new String(row[column+Integer.decode(PROPERTIES.getProperty("NUMBER_OF_PEOPLE_PER_PERSON"))]));
                    graphDB.addEdge(knows, personNodeId, friendNodeId, EdgeKind.BIDIRECTIONAL, (short)random.nextInt(10));
					column++;
				}
				else
				{
					topicNodeId = TOPICMAP.get(row[column]);
					AssociatedTo assoc = new AssociatedTo();					
					graphDB.addEdge(assoc, personNodeId, topicNodeId, EdgeKind.BIDIRECTIONAL, (short)random.nextInt(10));
					topicWeight++;
					column++;
				}
          	}
        }
       	csvReader.close();
	}

	/**
	 * Method to create document nodes in the graph database
	 */

	private static void createDocumentNodes(GraphDatabase graphDB) throws IOException
    {

		// Create documents and relate them to authors and viewers
		Random random = new Random();
		Map<String,Object> properties;
		long topicNodeId, personNodeId, documentNodeId;
		ArrayList<String> topics = CsvDataGenerator.load(PROPERTIES.getProperty("TOPICS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS")));
		ArrayList<String> names = CsvDataGenerator.load(PROPERTIES.getProperty("NAMES_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_PEOPLE")));
		Iterator<String> namesItr = names.iterator();
		Iterator<String> topicsItr = topics.iterator();
		for (int i=0; i < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_DOCUMENTS")); i++)
		{
		    Document doc = new Document();
			documentNodeId = graphDB.addVertex(doc);
	        for (int j = 0; j < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_AUTHORS")); j++)
	        {
				personNodeId = PERSONMAP.get(names.get(random.nextInt(names.size())));
				Authors auth = new Authors();					
				graphDB.addEdge(auth, personNodeId, documentNodeId, EdgeKind.BIDIRECTIONAL, (short)random.nextInt(10));
	        }
	        for (int k = 0; k < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_VIEWERS")); k++)
	        {
				personNodeId = PERSONMAP.get(names.get(random.nextInt(names.size())));
				Views views = new Views();					
				graphDB.addEdge(views, personNodeId, documentNodeId, EdgeKind.BIDIRECTIONAL, (short)random.nextInt(10));
	        }
	        for (int l = 0; l < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS_PER_DOCUMENT")); l++)
	        {
				topicNodeId = TOPICMAP.get(topics.get(random.nextInt(topics.size())));
				Has has = new Has();					
				graphDB.addEdge(has, topicNodeId, documentNodeId, EdgeKind.BIDIRECTIONAL, (short)random.nextInt(10));
	        }
		}
	}

	/**
     * Builds a map that holds string to nodeID connections
     * @param type the type of map to return; either person or topic
     * @param GraphDabase
     * @return a HashMap of strings to nodeIds
	 */

	private static HashMap<String,Long> getMap(String type, GraphDatabase graphDB)
	{
		HashMap<String,Long> map = new HashMap<String,Long>();
		Root referenceNode = (Root)graphDB.getNamedVertex("root");
		
		if (type.equals("topic"))
		{
		    Qualifier topicsFilter = new VertexTypes(graphDB.getTypeId(Topics.class.getName()));
            for (VertexHandle topicsNode : referenceNode.getNeighbors(topicsFilter))
            {
                Qualifier topicFilter = new VertexTypes(graphDB.getTypeId(Topic.class.getName()));
                for (VertexHandle topicNode : topicsNode.getVertex().getNeighbors(topicFilter))
                {
                    map.put(((Topic)topicNode.getVertex()).getTopic(), topicNode.getVertex().getId());
                }
		    }
		}
		else if (type.equals("person"))
		{
		    Qualifier peopleFilter = new VertexTypes(graphDB.getTypeId(People.class.getName()));
            for (VertexHandle peopleNode : referenceNode.getNeighbors(peopleFilter))
            {
                Qualifier personFilter = new VertexTypes(graphDB.getTypeId(Person.class.getName()));
                for (VertexHandle personNode : peopleNode.getVertex().getNeighbors(personFilter))
                {
                    map.put(((Person)personNode.getVertex()).getName(), personNode.getVertex().getId());
                }
		    }
		}
		return map;
	}

	/**
	 * Method to create document nodes in the graph database
	 */

	private static void generateCsvData() throws IOException
    {
		CsvDataGenerator sdg = new CsvDataGenerator(PROPERTIES);
	}


}








