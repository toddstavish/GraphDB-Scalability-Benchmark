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

import au.com.bytecode.opencsv.CSVReader;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import bench.utils.Stopwatch;
import bench.utils.CsvDataGenerator;

/**
 * This class is used to load the Neo4j Graph database.
 * It requires a configuration.properties file to learn the
 * evironment. Additionally, it outputs the text and CSV files
 * that comprise the data in the graph. Large vm heap sizes are
 * necessary for this to run well.
 * @param PROPERTIES  properties.configuration file
 * @param names.txt   census name list
 */

public class Neo4jIngest
{


    /**
     * Holds configuration properties on Neo4J, nodes and edge numbers, paths to CSV,
     * and graph.
	 */

	private static Properties PROPERTIES;

	/**
 	* Runtime map, resolves topic strings to nodeIds
 	*/

	private static HashMap<String,Long> TOPICMAP;

	/**
	* Runtime maps, resolves group strings to nodeIds
	*/

	private static HashMap<String,Long> GROUPMAP;

	/**
 	* Runtime maps, resolves person strings to nodeIds
 	*/

	private static HashMap<String,Long> PERSONMAP;

	/**
 	* Main method is used to load the Neo4J Graph database.
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
		createTopicNodes();
		createGroupNodes();
		createPeopleNodes();
		createDocumentNodes();
    }

	/**
	 * Method to create topic nodes in the graph database
	 */

	private static void createTopicNodes() throws IOException
    {
		String topic;
		long topicNodeId, topicNodesId, referenceNodeId = 0;
		BatchInserter neo = new BatchInserterImpl(PROPERTIES.getProperty("GRAPHDB_PATH"), BatchInserterImpl.loadProperties("configuration.properties"));
		Map<String,Object> properties = new HashMap<String,Object>();
		properties.put("type", "topics");
		topicNodesId = neo.createNode(properties);
		neo.createRelationship(referenceNodeId, topicNodesId, Neo4jRelationshipTypes.TOPICS, null);
		ArrayList<String> topics = CsvDataGenerator.load(PROPERTIES.getProperty("TOPICS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS")));
		Iterator<String> topicsItr = topics.iterator();
		TOPICMAP = new HashMap<String,Long>();
		while (topicsItr.hasNext())
		{
			topic = topicsItr.next();
			properties = new HashMap<String,Object>();
			properties.put("topic", topic);
			topicNodeId = neo.createNode(properties);
			TOPICMAP.put(topic, new Long(topicNodeId));
			neo.createRelationship(topicNodesId, topicNodeId, Neo4jRelationshipTypes.TOPIC, null);
	    }
		neo.shutdown();
	}

	/**
	 * Method to create group nodes in the graph database
	 */

	private static void createGroupNodes() throws IOException
    {
		String group;
		long groupNodeId, groupNodesId, referenceNodeId = 0;
		BatchInserter neo = new BatchInserterImpl(PROPERTIES.getProperty("GRAPHDB_PATH"), BatchInserterImpl.loadProperties("configuration.properties"));
		Map<String,Object> properties = new HashMap<String,Object>();
		properties.put("type", "groups");
		groupNodesId = neo.createNode(properties);
		neo.createRelationship(referenceNodeId, groupNodesId, Neo4jRelationshipTypes.GROUPS, null);
		ArrayList<String> groups = CsvDataGenerator.load(PROPERTIES.getProperty("GROUPS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_GROUPS")));
		Iterator<String> groupsItr = groups.iterator();
		GROUPMAP = new HashMap<String,Long>();
		while (groupsItr.hasNext())
		{
			group = groupsItr.next();
			properties = new HashMap<String,Object>();
			properties.put("name", group);
			groupNodeId = neo.createNode(properties);
			GROUPMAP.put(group, new Long(groupNodeId));
			neo.createRelationship(groupNodesId, groupNodeId, Neo4jRelationshipTypes.GROUP, null);
	    }
		neo.shutdown();
	}

	/**
	 * Method to create person nodes in the graph database
	 */

	private static void createPeopleNodes() throws IOException
    {
		// Create people nodes and attach them to root, put them into two groups randomly
	  	String name, group;
		Random random = new Random();
		long personNodeId, personNodesId, groupNodeId, referenceNodeId = 0;
		BatchInserter neo = new BatchInserterImpl(PROPERTIES.getProperty("GRAPHDB_PATH"), BatchInserterImpl.loadProperties("configuration.properties"));
		Map<String,Object> properties = new HashMap<String,Object>();
		properties.put("type", "people");
		personNodesId = neo.createNode(properties);
		neo.createRelationship(referenceNodeId, personNodesId, Neo4jRelationshipTypes.PEOPLE, null);
		ArrayList<String> names = CsvDataGenerator.load(PROPERTIES.getProperty("NAMES_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_PEOPLE")));
		ArrayList<String> groups = CsvDataGenerator.load(PROPERTIES.getProperty("GROUPS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_GROUPS")));
		ArrayList<String> topics = CsvDataGenerator.load(PROPERTIES.getProperty("TOPICS_PATH"), Integer.decode(PROPERTIES.getProperty("NUMBER_OF_TOPICS")));
		Iterator<String> namesItr = names.iterator();
		PERSONMAP = new HashMap<String,Long>();
	 	while (namesItr.hasNext())
		{
			name = namesItr.next();
			group = groups.get(random.nextInt(groups.size()));
			groupNodeId = GROUPMAP.get(group);
			properties = new HashMap<String,Object>();
			properties.put("name", name);
			personNodeId = neo.createNode(properties);
			PERSONMAP.put(name, new Long(personNodeId));
			neo.createRelationship(personNodesId, personNodeId, Neo4jRelationshipTypes.PERSON, null);
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
					neo.createRelationship(personNodeId, friendNodeId, Neo4jRelationshipTypes.KNOWS, properties);
					column++;
				}
				else
				{
					topicNodeId = TOPICMAP.get(row[column]);
					properties = new HashMap<String,Object>();
					properties.put("weight", random.nextInt(10));  // Weights randomly assigned between 1-10
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

	private static HashMap<String,Long> getMap(String type, GraphDatabaseService neo)
	{
		long referenceNodeId = 0;
		HashMap<String,Long> map = new HashMap<String,Long>();
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

	/**
     * Deletes the entire graph from the referenceNode down
	 */

    static void deleteGraphData(final GraphDatabaseService neo)
    {
        Transaction tx = neo.beginTx();
        try
        {
            Node refNode = neo.getReferenceNode();
            for (Node node : neo.getAllNodes())
            {
                if (node.equals(refNode))
                {
                    continue;
                }
                for (Relationship rel : node.getRelationships())
                {
                    rel.delete();
                }
                node.delete();
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

	/**
     * Prints out an object
	 */

    static void log(final Object s)
    {
        System.out.println(s);
    }

	/**
     * Prints out a list of Nodes
	 */

    static void printNodeList(final Iterable<Node> nodes)
    {
        for (Node node : nodes)
        {
            log(name(node));
        }
    }

	/**
     * Prints out the name of a person node
	 */

    private static String name(final Node node)
    {
        return (String) node.getProperty("name");
    }

	/**
     * Registers a hook so Neo can shutdown gracefully if the process is terminated
	 */

	static void registerShutdownHookForNeo(final GraphDatabaseService neo)
	{

		Runtime.getRuntime().addShutdownHook(new Thread()
	    {
	     	@Override
	        public void run()
	        {
	        	neo.shutdown();
	        }
	    } );
	}
}