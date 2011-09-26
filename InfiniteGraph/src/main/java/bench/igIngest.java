package bench;

import com.objy.db.app.ooId;

import com.infinitegraph.EdgeKind;
import com.infinitegraph.AccessMode;
import com.infinitegraph.Transaction;
import com.infinitegraph.GraphFactory;
import com.infinitegraph.GraphDatabase;
import com.infinitegraph.StorageException;
import com.infinitegraph.ConfigurationException;

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

	private static HashMap<String,ooId> TOPICMAP;

	/**
	* Runtime maps, resolves group strings to ooIds
	*/

	private static HashMap<String,ooId> GROUPMAP;

	/**
 	* Runtime maps, resolves person strings ooIds
 	*/

	private static HashMap<String,ooId> PERSONMAP;

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

		// Build graph
		loadDataToGraph();
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
        String propertiesFileName = "igIngest.properties";

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
            createTopicNodes(graphDB);
            createGroupNodes(graphDB);
            createPeopleNodes(graphDB);
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
		TOPICMAP = new HashMap<String,ooId>();
		while (topicsItr.hasNext())
		{
			topic = topicsItr.next();
			Topic topicNode = new Topic(topic);
		    graphDB.addVertex(topicNode);
			TOPICMAP.put(topic, topicNode.getOid());
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
		GROUPMAP = new HashMap<String,ooId>();
		while (groupsItr.hasNext())
		{
			group = groupsItr.next();
			Group groupNode = new Group(group);
			graphDB.addVertex(topicNode);
			GROUPMAP.put(group, groupNode.getOid());
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
		PERSONMAP = new HashMap<String,ooId>();
	 	while (namesItr.hasNext())
		{
			name = namesItr.next();
			group = groups.get(random.nextInt(groups.size()));
			groupNodeId = GROUPMAP.get(group);
			
			Person personNode = new Person(name);
			graphDB.addVertex(personNode);
			PERSONMAP.put(name, personNode.getOid());
			PersonEdge personEdge = new PersonEdge();
			peopleNode.addEdge(personEdge, personNode, EdgeKind.BIDIRECTIONAL);
						
			neo.createRelationship(personNodeId, groupNodeId, Neo4jRelationshipTypes.IS_MEMBER_OF, null);
			group = groups.get(random.nextInt(groups.size()));
			groupNodeId = GROUPMAP.get(group);
			neo.createRelationship(personNodeId, groupNodeId, Neo4jRelationshipTypes.IS_MEMBER_OF, null);
	    }

		// Associate people and topics to people
		String[] row;
		CSVReader csvReader = new CSVReader(new FileReader(PROPERTIES.getProperty("PEOPLE_PATH")));
		while ((row = csvReader.readNext()) != null)
		{
			int column = 0;
			personNodeId = 0;
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
					properties = new HashMap<String,Object>();
					properties.put("topic", new String(row[column+Integer.decode(PROPERTIES.getProperty("NUMBER_OF_PEOPLE_PER_PERSON"))]));
					properties.put("weight", random.nextInt(10)); // Weights randomly assigned between 1-10
					properties.put("cost", random.nextDouble()); // Cost randomly assigned between 0-1.0					
					neo.createRelationship(personNodeId, friendNodeId, Neo4jRelationshipTypes.KNOWS, properties);
					column++;
				}
				else
				{
					topicNodeId = TOPICMAP.get(row[column]);
					properties = new HashMap<String,Object>();
					properties.put("weight", random.nextInt(10));  // Weights randomly assigned between 1-10
					properties.put("cost", random.nextDouble()); // Cost randomly assigned between 0-1.0
					neo.createRelationship(personNodeId, topicNodeId, Neo4jRelationshipTypes.ASSOCIATED_TO, properties);
					topicWeight++;
					column++;
				}
          	}
        }
       	csvReader.close();
		neo.shutdown();
	}

	/**
	 * Method to create document nodes in the graph database
	 */

	private static void createDocumentNodes() throws IOException
    {
		// Create graph batch database inserter
		BatchInserter neo = new BatchInserterImpl(PROPERTIES.getProperty("GRAPHDB_PATH"), BatchInserterImpl.loadProperties("configuration.properties"));

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
			properties = new HashMap<String,Object>();
			documentNodeId = neo.createNode(properties);
	        for (int j = 0; j < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_AUTHORS")); j++)
	        {
				personNodeId = PERSONMAP.get(names.get(random.nextInt(names.size())));
			    neo.createRelationship(personNodeId, documentNodeId, Neo4jRelationshipTypes.AUTHORS, null);
	        }
	        for (int k = 0; k < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_VIEWERS")); k++)
	        {
				personNodeId = PERSONMAP.get(names.get(random.nextInt(names.size())));
				neo.createRelationship(personNodeId, documentNodeId, Neo4jRelationshipTypes.VIEWS, null);
	        }
	        for (int l = 0; l < Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS_PER_DOCUMENT")); l++)
	        {
				topicNodeId = TOPICMAP.get(topics.get(random.nextInt(topics.size())));
				neo.createRelationship(documentNodeId, topicNodeId, Neo4jRelationshipTypes.HAS, null);
	        }
		}
		neo.shutdown();
	}

	/**
     * Builds a map that holds string to nodeID connections
     * @param type the type of map to return; either person or topic
     * @param neo active Neo4J persistence service
     * @return a HashMap of strings to nodeIds
	 */

	private static HashMap<String,ooId> getMap(String type, GraphDatabase graphDB)
	{
		long referenceNodeId = 0;
		HashMap<String,Long> map = new HashMap<String,ooId>();
		Node referenceNode = neo.getNodeById(referenceNodeId);
		if (type.equals("topic"))
		{
			Node topicNodes = referenceNode.getSingleRelationship(Neo4jRelationshipTypes.TOPICS, Direction.OUTGOING).getEndNode();
			for (Relationship topic : topicNodes.getRelationships(Neo4jRelationshipTypes.TOPIC, Direction.OUTGOING))
	        {
				Node topicNode = topic.getEndNode();
				map.put(topicNode.getProperty("topic").toString(), topicNode.getId());
	        }
		}
		else if (type.equals("person"))
		{
			Node personNodes = referenceNode.getSingleRelationship(Neo4jRelationshipTypes.PEOPLE, Direction.OUTGOING).getEndNode();
			for (Relationship person : personNodes.getRelationships(Neo4jRelationshipTypes.PERSON, Direction.OUTGOING))
	        {
				Node personNode = person.getEndNode();
				map.put(personNode.getProperty("name").toString(), personNode.getId());
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








