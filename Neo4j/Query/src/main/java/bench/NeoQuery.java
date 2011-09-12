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

import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedWriter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import au.com.bytecode.opencsv.CSVReader;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import org.neo4j.graphalgo.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.centrality.EigenvectorCentralityPower;
import org.neo4j.graphalgo.centrality.EigenvectorCentralityArnoldi;

import org.neo4j.graphalgo.shortestpath.Dijkstra;
import org.neo4j.graphalgo.shortestpath.FloydWarshall;
import org.neo4j.graphalgo.shortestpath.std.DoubleAdder;
import org.neo4j.graphalgo.shortestpath.std.IntegerAdder;
import org.neo4j.graphalgo.shortestpath.std.DoubleEvaluator;
import org.neo4j.graphalgo.shortestpath.std.DoubleComparator;
import org.neo4j.graphalgo.shortestpath.std.IntegerEvaluator;
import org.neo4j.graphalgo.shortestpath.std.IntegerComparator;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathBFS;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.shortestpath.SingleSourceSingleSinkShortestPath;

import bench.utils.PersonDAO;
import bench.utils.Stopwatch;
import bench.utils.CsvDataGenerator;

/**
 * This class is used to query the Neo4J Graph database.
 * It requires a configuration.properties file to learn the
 * evironment.
 * @param PROPERTIES  properties.configuration file
 */

public class NeoQuery
{

    /**
     * Holds configuration properties on Neo4J, nodes and edge numbers, paths to CSV,
     * and graph. The query that is called is defined by the QUERY property. Additionally,
     * the query paratmeters (TOPIC, PERSON, WEIGHT) are also set here.
	 */

	private static Properties PROPERTIES;

	/**
     * Runtime map, resolves topic strings to nodeIds
	 */

	private static HashMap<String,Long> TOPICMAP;

	/**
     * Runtime maps, resolves person strings to nodeIds
	 */

	private static HashMap<String,Long> PERSONMAP;

	/**
	 * Main method is used to query the Neo4J Graph database.
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

		// Initialize Neo4J
		GraphDatabaseService neo = new EmbeddedGraphDatabase(PROPERTIES.getProperty("GRAPHDB_PATH"));
		registerShutdownHookForNeo(neo);
		Transaction tx = neo.beginTx();
    	try
    	{
			// Retrieve nodes from map for query parameters
			String name = new String(PROPERTIES.getProperty("PERSON"));
			String topic = new String(PROPERTIES.getProperty("TOPIC"));
			int weight = Integer.decode(PROPERTIES.getProperty("WEIGHT"));
			int breadth = Integer.decode(PROPERTIES.getProperty("BREADTH"));
			int query = Integer.decode(PROPERTIES.getProperty("QUERY"));
			System.out.println("Query parameters: Person = " +  name + "; Topic = " + topic + "; Weight = " + weight);
			if (TOPICMAP == null || PERSONMAP == null)
			{
				TOPICMAP = getMap("topic", neo);
				PERSONMAP = getMap("person", neo);
			}
			Node personNode, topicNode;

			// Query 1: Find all people associated with a topic.
			if (query == 1)
			{
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 1: Found people associated with topic = " + topic + ".");
				printNodeList( findPeopleOnTopic(topicNode) );
				findPeopleOnTopicJSON(topicNode);
			}
			// Query 2: Find all people associated with the topic with some weight.
			else if (query == 2)
			{
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 2: Found people associated with a topic = " + topic + ". Filtered by weight = " + weight + ".");
				printNodeList( findPeopleOnTopicFilterByWeight(topicNode, weight) );
				findPeopleOnTopicFilterByWeightJSON(topicNode, weight);
			}
			// Query 3: Find people associated to the person on the topic
			else if (query == 3)
			{
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 3: Found people associated with person " + name + " and topic " + topic + ".");
				printNodeList( findPeopleAssociatedToTheTopicOfAPerson(personNode, topicNode) );
				findPeopleAssociatedToTheTopicOfAPersonJSON(personNode, topicNode);
			}
			// Query 4: Find the people the person knows on this topic.
			else if (query == 4)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 4: Found all people the person " + name + " knows via topic " + topic + ".");
				printNodeList( findPeopleThatKnowAPersonByATopic(personNode, topic) );
				findPeopleThatKnowAPersonByATopicJSON(personNode, topic);
			}
			// Query 5: Find the people the person knows (1st level).
			else if (query == 5)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 5: Found all people the person " + name + " knows (1st level connections).");
				printNodeList( findPeopleThatKnowAPerson(personNode) );
				findPeopleThatKnowAPersonJSON(personNode); 
			}
			// Query 6: Find the people the person may know (2nd level).
			else if (query == 6)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 6: Found all people the person " + name + " may know (2nd level connections).");
				printNodeList( findPeopleThatMayKnowAPerson(personNode) );
				findPeopleThatMayKnowAPersonJSON(personNode);
				
			}
			// Query 7: Find the people the person knows on this topic and display group
			else if (query == 7)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 7: Found all people the person " + name + " knows via topic " + topic + ". Printing groups.");
				printNodeListWithAttribute( findPeopleThatKnowAPersonByATopic(personNode, topic) );
			}
			// Query 8: Find people associated to the person on the topic and display group
			else if (query == 8)
			{
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 8: Found people associated with person " + name + " and topic " + topic + ". Printing groups.");
				printNodeListWithAttribute( findPeopleAssociatedToTheTopicOfAPerson(personNode, topicNode) );
			}
			// Query 9: Find a person's groups
			else if (query == 9)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 9: Found all groups of person " + name);
				printNodeList( findGroups(personNode) );
				findGroupsJSON(personNode);
			}
			// Query 10: Find the people the person may know (n levels).
			else if (query == 10)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 10: Found all people the person " + name + " may know (n level connections).");
				printNodeList( findPeopleThatMayKnowAPerson(personNode, 2) );
				findPeopleThatMayKnowAPersonJSON(personNode, 2);
			}
			// Query 11: Find the people the person may know (2 levels hard coded).
			else if (query == 11)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 11: Found all people the person " + name + " knows and who they know (2nd level connections).");
				printNodeListList( findPeopleThatKnowAPersonAndFriends(personNode) );
			}
			// Query 12: Find the people the person may know (2 levels) then create JSON file with the information.
			else if (query == 12)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 12: Found all people the person " + name + " knows and who they know (2nd level connections w/ breadth of " + breadth + ").");
				findPeopleThatKnowAPersonAndFriendsJSON(personNode);
			}
			// Query 13: Find the people the person may know (2 levels hard coded w/ breadth limiter).
			else if (query == 13)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 13: Found all people the person " + name + " knows and who they know (2nd level connections) with 1st and 2nd level breadth of " + breadth + ".");
				printNodeListList( findPeopleThatKnowAPersonAndFriends(personNode, breadth) );
				findPeopleThatKnowAPersonAndFriendsJSON(personNode, breadth);
			}
			// Query 14: Find the number of incoming edges of a person over a topic
			else if (query == 14)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 14: Found the number of incoming edges for " + name + " and topic " + topic + ".");
				System.out.println("Number of incoming edges: " + findNumberOfIncomingEdges(personNode, topic) );
			}
			// Query 15: Find the number of outgoing edges of a person over a topic
			else if (query == 15)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 15: Found the number of outgoing edges for " + name + " and topic " + topic + ".");
				System.out.println("Number of outgoing edges: " + findNumberOfOutgoingEdges(personNode, topic) );
			}
			// Query 16: Update a person's topic weight using outgoing edge count
			else if (query == 16)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				System.out.println("Query 16: Found the number of outgoing edges for " + name + " and topic " + topic + ".");
				int count = findNumberOfOutgoingEdges(personNode, topic);
				System.out.println("Number of outgoing edges: " +  count);
				updateTopicWeight(personNode, topic, count);
			}
			// Query 17: Calculate betweeness centrality using Breadth First Search
			else if (query == 17)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 17: Calculate betweeness centrality using Breadth First Search for " + name + " and topic " + topic + ".");
				double count = findBetweenessCentralityBFS(personNode, topic);
			}
			// Query 18: Calculate shortest path using Dijkstra
			else if (query == 18)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 18: Finding all shortest paths using Dijkstra for " + name + " and topic " + topic + ".");
				findDijkstraShortestPath(personNode, topic);
			}
			// Query 19: Calculate shortest paths using Floyd-Warshall
			else if (query == 19)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 19: Calculate shortest paths using Floyd-Warshall for " + name + " and topic " + topic + ".");
				findFloydWarshallShortestPath(personNode, topicNode);
			}
			// Query 20: Calculate Eigenvector Centrality with the "Arnoldi iteration"
			else if (query == 20)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 20: Calculate Eigenvector centrality using Arnoldi iteration for " + name + " and topic " + topic + ".");
				double count = findEigenvectorCentralityArnoldi(personNode, topicNode);
			}
			// Query 21: Calculate Eigenvector Centrality with the "Power method"
			else if (query == 21)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 21: Calculate Eigenvector centrality using Power Method for " + name + " and topic " + topic + ".");
				double count = findEigenvectorCentralityPower(personNode, topicNode);
			}
			// Query 22: Calculate shortest paths using BFS
			else if (query == 22)
			{
				personNode = neo.getNodeById(PERSONMAP.get(name));
				topicNode = neo.getNodeById(TOPICMAP.get(topic));
				System.out.println("Query 22: Calculate shortest paths using Breadth First for " + name + " and topic " + topic + ".");
				findBreadthFirstShortestPath(personNode, topic);
			}
		}
		finally
       	{
           	tx.finish();
       	}

		//deleteGraphData(neo);

		// Shutdown Neo4J
        neo.shutdown();
	}

	/**
     * Queries to find people related to a topic.
     * @param topic the node of the topic to be queried
     * @return a list of the people related to that topic
	 */

	public static List<Node> findPeopleOnTopic(final Node topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

        List<Node> people = new ArrayList<Node>();
        for (Relationship associatedTo : topic.getRelationships(RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING))
        {
			Node person = associatedTo.getStartNode();
            people.add(person);
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried all people related to topic " + topic.getProperty("topic") + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + people.size() + " people.");
        return people;
    }

	public static List<Relationship> findTopicRelationships(final Node topic)
    {

        List<Relationship> rels = new ArrayList<Relationship>();
        for (Relationship associatedTo : topic.getRelationships(RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING))
        {
            rels.add(associatedTo);			
        }

        return rels;
    }

    public static void findPeopleOnTopicJSON(final Node topic) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();
		
		PersonDAO topicDAO = new PersonDAO();
		topicDAO.name = (String)topic.getProperty("topic");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(topicDAO);
        for (Relationship associatedTo : topic.getRelationships(RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING))
        {
			Node person = associatedTo.getStartNode();
			topicDAO.imports.add((String)person.getProperty("name"));
  			PersonDAO personDAO = new PersonDAO();
			personDAO.name = (String)person.getProperty("name");
			everyone.add(personDAO);
        }

		// Stop timer
	    timer.stop();
	    System.out.println("JSON'd: Queried " + topic.getProperty("topic") + " to see the people that connected in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Queries to find people related to a topic and then filters out
     * the people without the given weight
     * @param topic the node of the topic to be queried
     * @param weight the weight of the topic of interest
     * @return a list of the people related to that topic with the given weight
	 */

	public static List<Node> findPeopleOnTopicFilterByWeight(final Node topic, final int weight)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

        List<Node> people = new ArrayList<Node>();
        for (Relationship associatedTo : topic.getRelationships(RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING))
        {
			Node person = associatedTo.getStartNode();
			Integer relWeight = (Integer)associatedTo.getProperty("weight");
			if (weight == relWeight.intValue())
			{
            	people.add(person);
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried all people related to topic " + topic.getProperty("topic") + " with weight " + weight + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + people.size() + " people.");
        return people;
    }

	public static void findPeopleOnTopicFilterByWeightJSON(final Node topic, final int weight)  throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		PersonDAO topicDAO = new PersonDAO();
		topicDAO.name = (String)topic.getProperty("topic");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(topicDAO);
        for (Relationship associatedTo : topic.getRelationships(RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING))
        {
			Node person = associatedTo.getStartNode();
			Integer relWeight = (Integer)associatedTo.getProperty("weight");
			if (weight == relWeight.intValue())
			{
				topicDAO.imports.add((String)person.getProperty("name"));
	  			PersonDAO personDAO = new PersonDAO();
				personDAO.name = (String)person.getProperty("name");
				everyone.add(personDAO);
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("JSON'd: Queried " + topic.getProperty("topic") + " to see the people that connected in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Queries to find people that are associated to the same topic
     * as the provided person
     * @param person the node of the person to be queried
     * @param topic the node of the topic that is associated
     * @return a list of the people associated to the same topic
	 */

	public static List<Node> findPeopleAssociatedToTheTopicOfAPerson(final Node person, final Node topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		// Instantiate a traverser that returns a person's topics
		Traverser peopleTraverser = topic.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING);


		// Traverse the node space
		boolean personHasTopic = false;
		List<Node> people = new ArrayList<Node>();
		String topicStr = (String) topic.getProperty("topic");
		for (Node  otherPerson : peopleTraverser)
		{
			if(!otherPerson.getProperty("name").equals(person.getProperty("name")))
			{
				people.add(otherPerson);
			}
			else
			{
				personHasTopic = true;
			}
		}

		// Return results
		timer.stop();
		System.out.println("Queried all people related to " + person.getProperty("name") + " on topic " + topic.getProperty("topic") + " in "+ timer.getElapsedTime() + " in milliseconds.");
		System.out.println("Found: " + people.size() + " people.");

		// Only return the list if the person has the topic
		if (personHasTopic)
		{
			return people;
		}
		else
		{
			return null;
		}
    }

	public static void findPeopleAssociatedToTheTopicOfAPersonJSON(final Node person, final Node topic) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		// Instantiate a traverser that returns a person's topics
		Traverser peopleTraverser = topic.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, RelationshipTypes.ASSOCIATED_TO, Direction.INCOMING);

		// Traverse the node space
		boolean personHasTopic = false;
		PersonDAO personDAO = new PersonDAO();
		personDAO.name = (String)person.getProperty("name");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
		String topicStr = (String) topic.getProperty("topic");
		for (Node  otherPerson : peopleTraverser)
		{
			if(!otherPerson.getProperty("name").equals(person.getProperty("name")))
			{
				personDAO.imports.add((String)otherPerson.getProperty("name"));
	  			PersonDAO friendDAO = new PersonDAO();
				friendDAO.name = (String)otherPerson.getProperty("name");
				everyone.add(friendDAO);
			}
			else
			{
				personHasTopic = true;
			}
		}

		// Return results
		timer.stop();
		System.out.println("JSON'd Queried all people related to " + person.getProperty("name") + " on topic " + topic.getProperty("topic") + " in "+ timer.getElapsedTime() + " in milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Queries to find people related to a person along a known topic relationship
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @return a list of the people related to that person along a known topic
	 */

	public static List<Node> findPeopleThatKnowAPersonByATopic(final Node person, final String topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

        List<Node> people = new ArrayList<Node>();
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			if (topic.equals(knows.getProperty("topic")))
			{
            	people.add(friend);
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + people.size() + " people.");
        return people;
    }

	public static void findPeopleThatKnowAPersonByATopicJSON(final Node person, final String topic) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		boolean personHasTopic = false;
		PersonDAO personDAO = new PersonDAO();
		personDAO.name = (String)person.getProperty("name");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			if (topic.equals(knows.getProperty("topic")))
			{
				personDAO.imports.add((String)friend.getProperty("name"));
	  			PersonDAO friendDAO = new PersonDAO();
				friendDAO.name = (String)friend.getProperty("name");
				everyone.add(friendDAO);
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("JSON'd Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Queries to find people that a person knows (1st level connections)
     * @param person the node of the person to be queried
     * @return a list of the people the person knows
	 */

	public static List<Node> findPeopleThatKnowAPerson(final Node person)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

        List<Node> people = new ArrayList<Node>();
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
            people.add(friend);
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + people.size() + " people.");
        return people;
    }

    public static void findPeopleThatKnowAPersonJSON(final Node person) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();
		
		PersonDAO personDAO = new PersonDAO();
		personDAO.name = (String)person.getProperty("name");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {		
			Node friend = knows.getOtherNode(person);
			personDAO.imports.add((String)friend.getProperty("name"));
			PersonDAO friendDAO = new PersonDAO();
			friendDAO.name = (String)friend.getProperty("name");
			everyone.add(friendDAO);
        }

		// Stop timer
	    timer.stop();
	    System.out.println("JSON'd: Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Queries to find people that a person *may* know (2nd level connections)
     * @param person the node of the person to be queried
     * @return a list of the people that the person may know
	 */

	public static List<Node> findPeopleThatMayKnowAPerson(final Node person)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		Set<Node> people = new HashSet<Node>();
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			for (Relationship mayKnow : friend.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
			{
				Node stranger = mayKnow.getOtherNode(friend);
				people.add(stranger);
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + people.size() + " people.");
        return new ArrayList<Node>(people);
    }

    static void findPeopleThatMayKnowAPersonJSON(Node person) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();
		
		PersonDAO personDAO = new PersonDAO();
		personDAO.name = (String)person.getProperty("name");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {		
			Node friend = knows.getOtherNode(person);
			personDAO.imports.add((String)friend.getProperty("name"));
			PersonDAO friendDAO = new PersonDAO();
			friendDAO.name = (String)friend.getProperty("name");
			everyone.add(friendDAO);
			for (Relationship mayKnow : friend.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING))
			{
				Node foaf = mayKnow.getOtherNode(friend);
				friendDAO.imports.add((String)foaf.getProperty("name"));
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("JSON'd: Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }


	/**
     * Queries to find a person's groups
     * @param person the node of the person to be queried
     * @return a list of the person's groups
	 */

	public static List<Node> findGroups(final Node person)
    {

        List<Node> groups = new ArrayList<Node>();
        for (Relationship memberOf : person.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
        {
			Node group = memberOf.getOtherNode(person);
            groups.add(group);
        }

        return groups;
    }

	public static void findGroupsJSON(final Node person) throws IOException
    {

		PersonDAO personDAO = new PersonDAO();
		personDAO.name = (String)person.getProperty("name");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
        for (Relationship memberOf : person.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
        {
			Node group = memberOf.getOtherNode(person);
			personDAO.imports.add((String)group.getProperty("name"));
			PersonDAO groupDAO = new PersonDAO();
			groupDAO.name = (String)group.getProperty("name");
			everyone.add(groupDAO);
        }
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Queries to find people that a person knows and their friends (2nd level connections)
     * @param person the node of the person to be queried
     * @return a list of lists of the people that the person may know
	 */

	public static List<List<Node>>  findPeopleThatKnowAPersonAndFriends(final Node person)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		Set<List<Node>> friends = new HashSet<List<Node>>();
		Set<Node> people = new HashSet<Node>();
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			people.add(friend);
			for (Relationship mayKnow : friend.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
			{

				Node stranger = mayKnow.getOtherNode(friend);
				people.add(stranger);
			}
			friends.add(new ArrayList<Node>(people));
			people = new HashSet<Node>();
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + friends.size() + " friends.");
        return new ArrayList<List<Node>>(friends);
    }


   	/**
	 * Queries to find people that a person knows \ may know (n level connections)
	 * @param person the node of the person to be queried
	 * @return a list of the people the person knows \ may know
	 */

	public static List<Node> findPeopleThatMayKnowAPerson(final Node person, final int level)
	{

		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		// Instantiate a traverser
		Traverser peopleTraverser = person.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, RelationshipTypes.KNOWS, Direction.BOTH);


		// Traverse the node space
		Set<Node> people = new HashSet<Node>();
		for (Node  otherPerson : peopleTraverser)
		{
			if(!otherPerson.getProperty("name").equals(person.getProperty("name")))
			{
				Integer depth = (Integer)peopleTraverser.currentPosition().depth();
				if (depth.intValue() == level+1)
				{
					timer.stop();
					System.out.println("Queried all people " + person.getProperty("name") + " knows to depth " + level + " in "+ timer.getElapsedTime() + " in milliseconds.");
					System.out.println("Found: " + people.size() + " people.");
					return new ArrayList<Node>(people);
				}
				people.add(otherPerson);
			}
		}

		// Return results
		return new ArrayList<Node>(people);

    }

	public static void findPeopleThatMayKnowAPersonJSON(final Node person, final int level) throws IOException
	{

		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		// Instantiate a traverser
		Traverser peopleTraverser = person.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, RelationshipTypes.KNOWS, Direction.OUTGOING);

		// Traverse the node space
		PersonDAO personDAO = new PersonDAO();
		personDAO.name = (String)person.getProperty("name");
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
		for (Node  otherPerson : peopleTraverser)
		{
			if(!otherPerson.getProperty("name").equals(person.getProperty("name")))
			{
				Integer depth = (Integer)peopleTraverser.currentPosition().depth();
				if (depth.intValue() == level+1)
				{
					timer.stop();
					System.out.println("JSON'd Queried all people " + person.getProperty("name") + " knows to depth " + level + " in "+ timer.getElapsedTime() + " in milliseconds.");
					Gson gson = new Gson();
					String json = gson.toJson(everyone);
					BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
					topicsBuffer.write(json);
					topicsBuffer.close();
					return;
				}
				personDAO.imports.add((String)otherPerson.getProperty("name"));
				PersonDAO friendDAO = new PersonDAO();
				friendDAO.name = (String)otherPerson.getProperty("name");
				everyone.add(friendDAO);
				for (Relationship mayKnow : otherPerson.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING))
				{
					Node foaf = mayKnow.getOtherNode(otherPerson);
					friendDAO.imports.add((String)foaf.getProperty("name"));
				}
			}
		}
		return;
    }

	/**
     * Queries to find people that a person knows and their friends (2nd level connections)
     * @param person the node of the person to be queried
     * @return a file with JSON graph of friends
	 */

	public static void  findPeopleThatKnowAPersonAndFriendsJSON(final Node person) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();
		
		PersonDAO personDAO = new PersonDAO();
		personDAO.size = 3500;
		String name = new String();
		for (Relationship memberOf : person.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
        {
			Node group = memberOf.getOtherNode(person);
			name = new String("flare" + "," + (String)group.getProperty("name") + "," + (String)person.getProperty("name"));
			
		}
		personDAO.name = name;
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			PersonDAO friendDAO = new PersonDAO();
			friendDAO.size = 5500;
			for (Relationship memberOf : friend.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
	        {
				Node group = memberOf.getOtherNode(friend);
				name = new String("flare" + "," + (String)group.getProperty("name") + "," + (String)friend.getProperty("name"));

			}	
			friendDAO.name = name;
			personDAO.imports.add(friendDAO.name);
			everyone.add(friendDAO);
			for (Relationship mayKnow : friend.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
			{
				Node stranger = mayKnow.getOtherNode(friend);
				for (Relationship memberOf : stranger.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
		        {
					Node group = memberOf.getOtherNode(stranger);
					name = new String("flare" + "," + (String)group.getProperty("name") + "," + (String)stranger.getProperty("name"));
				}	
				friendDAO.imports.add(name);
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();			
    }

	/**
     * Queries to find people that a person knows and their friends (2nd level connections) with breadth limiter
     * @param person the node of the person to be queried
     * @param breadth width of network (both levels)
     * @return a list of lists of the people that the person may know
	 */

	public static List<List<Node>> findPeopleThatKnowAPersonAndFriends(final Node person, final int breadth)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		Set<List<Node>> friends = new HashSet<List<Node>>();
		Set<Node> people = new HashSet<Node>();
		int outerBreadthCount = 0;
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			people.add(friend);
			int innerBreadthCount = 0;
			for (Relationship mayKnow : friend.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
			{
				Node stranger = mayKnow.getOtherNode(friend);
				people.add(stranger);
				innerBreadthCount++;
	    		if (innerBreadthCount == breadth)
	    		{
					break;
				}
			}
			friends.add(new ArrayList<Node>(people));
			people = new HashSet<Node>();
			outerBreadthCount++;
	    	if (outerBreadthCount == breadth)
	    	{
				break;
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + friends.size() + " friends.");
        return new ArrayList<List<Node>>(friends);
    }

	public static void findPeopleThatKnowAPersonAndFriendsJSON(final Node person, final int breadth) throws IOException
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		PersonDAO personDAO = new PersonDAO();
		personDAO.size = 3500;
		String name = new String();
		for (Relationship memberOf : person.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
        {
			Node group = memberOf.getOtherNode(person);
			name = new String("flare" + "," + (String)group.getProperty("name") + "," + (String)person.getProperty("name"));
			
		}
		personDAO.name = name;
		List<PersonDAO> everyone = new ArrayList<PersonDAO>();
		everyone.add(personDAO);
		int outerBreadthCount = 0;
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			PersonDAO friendDAO = new PersonDAO();
			friendDAO.size = 5500;
			for (Relationship memberOf : friend.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
	        {
				Node group = memberOf.getOtherNode(friend);
				name = new String("flare" + "," + (String)group.getProperty("name") + "," + (String)friend.getProperty("name"));

			}	
			friendDAO.name = name;
			personDAO.imports.add(friendDAO.name);
			everyone.add(friendDAO);			
			int innerBreadthCount = 0;
			for (Relationship mayKnow : friend.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
			{
				Node stranger = mayKnow.getOtherNode(friend);
				for (Relationship memberOf : stranger.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
		        {
					Node group = memberOf.getOtherNode(stranger);
					name = new String("flare" + "," + (String)group.getProperty("name") + "," + (String)stranger.getProperty("name"));
				}	
				friendDAO.imports.add(name);
				innerBreadthCount++;
	    		if (innerBreadthCount == breadth)
	    		{
					break;
				}
			}			
			outerBreadthCount++;
	    	if (outerBreadthCount == breadth)
	    	{
				break;
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("JSON'd Queried " + person.getProperty("name") + " to see the people that are known in "+ timer.getElapsedTime() + " milliseconds.");
		Gson gson = new Gson();
		String json = gson.toJson(everyone);
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(PROPERTIES.getProperty("JSON_PATH")));	
		topicsBuffer.write(json);
		topicsBuffer.close();
    }

	/**
     * Counts the number of incoming edges a person has over a topic
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @return the number of incoming edges
	 */

	public static int findNumberOfIncomingEdges(final Node person, final String topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

        int count = 0;
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.INCOMING))
        {
			Node friend = knows.getOtherNode(person);
			if (topic.equals(knows.getProperty("topic")))
			{
            	count++;
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + count + " incoming people.");
        return count;
    }

	/**
     * Counts the number of outgoing edges a person has over a topic
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @return the number of outgoing edges
	 */

	public static int findNumberOfOutgoingEdges(final Node person, final String topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

        int count = 0;
        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING))
        {
			Node friend = knows.getOtherNode(person);
			if (topic.equals(knows.getProperty("topic")))
			{
            	count++;
			}
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Found: " + count + " outgoing people.");
        return count;
    }

	/**
     * Queries to find people related to a person along a known topic relationship (then changes the weight of the relationship)
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @param newWeight changes the existing weight to this value
	 */

	public static void updateTopicWeight(final Node person, final String topic, final int newWeight)
    {

        for (Relationship knows : person.getRelationships(RelationshipTypes.KNOWS, Direction.BOTH))
        {
			Node friend = knows.getOtherNode(person);
			if (topic.equals(knows.getProperty("topic")))
			{
            	knows.setProperty("weight", newWeight);
			}
        }
    }

	/**
     * Finds centrality using Breath First Search
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @return centrality measure
	 */

	public static double findBetweenessCentralityBFS(final Node person, final String topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		List<Node> nodeList = findPeopleThatKnowAPersonByATopic(person,topic);
		printNodeList(nodeList);

		Set<Node> nodeSet = new HashSet<Node>(nodeList); 
		
        // Set up shortest path algorithm.
        // Observe that we don't need to specify a start node.
        SingleSourceShortestPath<Integer> singleSourceShortestPath;
        singleSourceShortestPath = new SingleSourceShortestPathBFS(
            null,
            Direction.BOTH,
            RelationshipTypes.KNOWS);

        // Set up betweenness centrality algorithm.
        BetweennessCentrality<Integer> betweennessCentrality;
        betweennessCentrality = new BetweennessCentrality<Integer>(
            singleSourceShortestPath, 
            nodeSet);
			
        // Get centrality value for a node.
        Double centrality = betweennessCentrality.getCentrality(person);

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("BFS Centrality: " + centrality);
        return centrality;
    }

	public static void findBreadthFirstShortestPath(final Node person, final String topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		List<Node> nodeList = findPeopleThatKnowAPersonByATopic(person,topic);
		Set<Node> nodeSet = new HashSet<Node>(nodeList); 
		printNodeList(nodeList);
		
        // Set up shortest path algorithm.
        // Observe that we don't need to specify a start node.
        SingleSourceShortestPath<Integer> singleSourceShortestPath;
        singleSourceShortestPath = new SingleSourceShortestPathBFS(
            person,
            Direction.OUTGOING,
            RelationshipTypes.KNOWS);

		// Calculate shortest's paths based on BFS
		SingleSourceShortestPathBFS singleSourceShortestPathBFS = (SingleSourceShortestPathBFS)singleSourceShortestPath;
		singleSourceShortestPathBFS.limitDepth(new Long(1));

		// Print out shortest paths
		System.out.println("Breadth First: printing out all shorest paths for query set of " + person.getProperty("name") + " related to topic " + topic + ".");
		for (Node node : nodeList)
		{
			List<PropertyContainer> path = singleSourceShortestPath.getPath(node);
			printPropertyContainerList(path);
		}
		
		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
    }
	
	/**
     * Find Dijkstra shortest path
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
	 */

	public static void findDijkstraShortestPath(final Node person, final String topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		List<Node> nodeList = findPeopleThatKnowAPersonByATopic(person,topic);
		printNodeList(nodeList);

        // Set up Dijkstra
        SingleSourceShortestPath<Integer> sp;
        sp = new SingleSourceShortestPathDijkstra<Integer>(
            0, 
            person, 
            new IntegerEvaluator("weight"),
            new IntegerAdder(), 
            new IntegerComparator(), 
            Direction.OUTGOING,
            RelationshipTypes.KNOWS);

        // Print out shortest paths
	    System.out.println("Dijkstra: printing out all shorest paths for query set of " + person.getProperty("name") + " related to topic " + topic + ".");
      	for (Node node : nodeList)
        {
			List<PropertyContainer> path = sp.getPath(node);
			printPropertyContainerList(path);
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
    }


	/**
     * Calculate shortest path using Floyd-Warshall
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
	 */

	public static void findFloydWarshallShortestPath(final Node person, final Node topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		List<Node> nodeList = findPeopleOnTopic(topic);
		printNodeList(nodeList);
		nodeList.add(topic);
		List<Relationship> relList = findTopicRelationships(topic);
       		
		Set<Node> nodeSet = new HashSet<Node>(nodeList);
		Set<Relationship> relSet = new HashSet<Relationship>(relList);
			
        // Set up Floyd Warshall
        FloydWarshall<Integer> fw;
        fw = new FloydWarshall<Integer>(
            1, 
            10,
			Direction.BOTH, 
            new IntegerEvaluator("weight"),
            new IntegerAdder(), 
            new IntegerComparator(), 
            nodeSet,
			relSet);
			
        // Print out shortest paths
	    System.out.println("Floyd Warshall: printing out all shorest paths for query set of " + person.getProperty("name") + " related to topic " + topic.getProperty("topic") + ".");
		fw.calculate();
		int depth = 0;
		int depthLimit = 1;
		List<Node> path = new ArrayList<Node>();
     	for (Node friend : nodeList)
        {	
			if (depth < depthLimit)
			{
	        	path = fw.getPath(person, friend);
				for (Node node : path)
				{
					System.out.println(node);
				}
			}
			depth++;
        }

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
    }

	/**
     * Finds centrality using Eigenvector Centrality with the Arnoldi iteration
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @return centrality measure
	 */

	public static double findEigenvectorCentralityArnoldi(final Node person, final Node topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		List<Node> nodeList = findPeopleOnTopic(topic);
		printNodeList(nodeList);
		List<Relationship> relList = findTopicRelationships(topic);
           		
		Set<Node> nodeSet = new HashSet<Node>(nodeList); 
		Set<Relationship> relSet = new HashSet<Relationship>(relList);
		
        // Set up Eingenvector Centrality with Arnoldi
        EigenvectorCentralityArnoldi ev;
        ev = new EigenvectorCentralityArnoldi(
            Direction.BOTH,
            new DoubleEvaluator("cost"),
			nodeSet,
			relSet,
            0.01);

		ev.setMaxIterations(50);
		ev.calculate();

        // Get centrality value for a node.
       	Double centrality = ev.getCentrality(person);

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Eigenvector Arnoldi Centrality: " + centrality);
        return centrality;
    }		
		
	/**
     * Finds centrality using Eigenvector Centrality with the Power Method
     * @param person the node of the person to be queried
     * @param topic the node of the topic that relates the person nodes
     * @return centrality measure
	 */
	public static double findEigenvectorCentralityPower(final Node person, final Node topic)
    {
		// Start timer
		Stopwatch timer = new Stopwatch();
		timer.start();

		List<Node> nodeList = findPeopleOnTopic(topic);
		printNodeList(nodeList);
		List<Relationship> relList = findTopicRelationships(topic);
           		
		Set<Node> nodeSet = new HashSet<Node>(nodeList); 
		Set<Relationship> relSet = new HashSet<Relationship>(relList);

        // Set up Eingenvector Centrality with Arnoldi
        EigenvectorCentralityPower ev;
        ev = new EigenvectorCentralityPower(
            Direction.BOTH,
            new DoubleEvaluator("cost"),
			nodeSet,
			relSet,
            0.001);

		ev.setMaxIterations(50);
		ev.calculate();
		
        // Get centrality value for a node.
        Double centrality = ev.getCentrality(person);

		// Stop timer
	    timer.stop();
	    System.out.println("Queried " + person.getProperty("name") + " related to topic " + topic + " in "+ timer.getElapsedTime() + " milliseconds.");
		System.out.println("Eigenvector Power Centrality: " + centrality);
        return centrality;
    }	
	
	
	/**
     * Builds a map that holds string to nodeID connections
     * @param type the type of map to return; either person or topic
     * @param neo active Neo4J persistence service
     * @return a HashMap of strings to nodeIds
	 */

	public static HashMap<String,Long> getMap(String type, GraphDatabaseService neo)
	{
		long referenceNodeId = 0;
		HashMap<String,Long> map = new HashMap<String,Long>();
		Node referenceNode = neo.getNodeById(referenceNodeId);
		if (type.equals("topic"))
		{
			Node topicNodes = referenceNode.getSingleRelationship(RelationshipTypes.TOPICS, Direction.OUTGOING).getEndNode();
			for (Relationship topic : topicNodes.getRelationships(RelationshipTypes.TOPIC, Direction.OUTGOING))
	        {
				Node topicNode = topic.getEndNode();
				map.put(topicNode.getProperty("topic").toString(), topicNode.getId());
	        }
		}
		else if (type.equals("person"))
		{
			Node personNodes = referenceNode.getSingleRelationship(RelationshipTypes.PEOPLE, Direction.OUTGOING).getEndNode();
			for (Relationship person : personNodes.getRelationships(RelationshipTypes.PERSON, Direction.OUTGOING))
	        {
				Node personNode = person.getEndNode();
				map.put(personNode.getProperty("name").toString(), personNode.getId());
	        }
		}
		return map;
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
     * Prints out a list of PropertyContainers
	 */

    static void printPropertyContainerList(final Iterable<PropertyContainer> pcs)
    {
        for (PropertyContainer pc : pcs)
        {
            log(pc);
        }
    }

	/**
     * Prints out a list of lists of Nodes
	 */

    static void printNodeListList(final Iterable<List<Node>> list)
    {
		for (List<Node> nodes : list)
		{
        	for (Node node : nodes)
        	{
        	    log(name(node));
        	}
		}
    }


	/**
     * Prints out a list of Nodes and Attributes
	 */

    static void printNodeListWithAttribute(final Iterable<Node> nodes)
    {
        for (Node node : nodes)
        {
            log(name(node));
			group(node);
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
     * Prints out the group name of a person node
	 */

    private static void group(final Node node)
    {
	    for (Relationship memberOf : node.getRelationships(RelationshipTypes.IS_MEMBER_OF, Direction.OUTGOING))
        {
			Node group = memberOf.getOtherNode(node);
			log(group.getProperty("name"));
        }
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