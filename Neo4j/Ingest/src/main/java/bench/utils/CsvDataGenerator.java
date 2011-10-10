package bench.utils;

import java.util.Set;
import java.util.List;
import java.util.Random;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Properties;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import rita.wordnet.RiWordnet;

/**
 * CSV and Text data generators
 */

public class CsvDataGenerator
{
	private List<String> names;
	private List<String> topics;
	private List<String> groups;
	private Properties properties;

	/**
	 * Constructor generates all CSV and Text files
	 */
	
	public CsvDataGenerator(Properties properties) throws IOException
	{
		// Properties
		this.properties = properties;
				
		// Create random topics (total amount is defined by properties)
		//topics = createTopics(Integer.decode(properties.getProperty("NUMBER_OF_TOPICS")));
		topics = load(properties.getProperty("TOPICS_PATH"), Integer.decode(properties.getProperty("NUMBER_OF_TOPICS")));
		//writeTopics();

		// Create unique people (total amount is defined by properties)
		names = load(properties.getProperty("NAMES_PATH"), Integer.decode(properties.getProperty("NUMBER_OF_PEOPLE")));
		writePeople();
	}
	
	private CsvDataGenerator() {}
	
	/**
	 * Writes topics out to a text file.
	 */
	
	private void writeTopics() throws IOException
	{
		Iterator topicsItr = topics.iterator();	
		BufferedWriter topicsBuffer = new BufferedWriter(new FileWriter(properties.getProperty("TOPICS_PATH")));	
		while (topicsItr.hasNext())
		{	
			topicsBuffer.write(topicsItr.next().toString());
			topicsBuffer.newLine();
		}
		topicsBuffer.close();			
	}
	
	/**
	 * Writes people out to a CSV file.
	 */
	
	private void writePeople() throws IOException
	{
		String name, topic, friend;
		StringBuilder person;
		Set<String> friends, subjects;
		Random random = new Random();
		Iterator<String> namesItr = names.iterator();		
		BufferedWriter peopleBuffer = new BufferedWriter(new FileWriter(properties.getProperty("PEOPLE_PATH")));	
		while (namesItr.hasNext())
		{	
			name = namesItr.next();
			person = new StringBuilder();
			person.append(name);
			friends = new HashSet<String>();
			// Who the person knows
			int numberOfFriends = 0;
		 	while (numberOfFriends < Integer.decode(properties.getProperty("NUMBER_OF_PEOPLE_PER_PERSON"))) 
			{	
				friend = names.get(random.nextInt(names.size()));
				if (friends.add(friend) && !friend.equals(name))
				{
					person.append("," + friend);
					numberOfFriends++;
				}
			}
			// Person's topics
			int numberOfTopics = 0;
			subjects = new HashSet<String>();
		 	while (numberOfTopics < Integer.decode(properties.getProperty("NUMBER_OF_TOPICS_PER_PERSON"))) 
			{	
				topic = topics.get(random.nextInt(topics.size()));
				if (subjects.add(topic))
				{
					person.append("," + topic);
					numberOfTopics++;
				}
			}
			peopleBuffer.write(person.toString());
			peopleBuffer.newLine();
		}
		peopleBuffer.close();
	}
	
	/**
	 * Generates the random topics list. Topics are different each iteration.
	 */
	
	public static ArrayList<String> createTopics(final int numberOfTopics)
	{
		// Create random topics (total amount is defined by properties)
		String topic;
		Random random = new Random();
		RiWordnet wordnet = new RiWordnet(null);
		Set<String> topicSet = new HashSet<String>();
		while (topicSet.size() < numberOfTopics) 
		{
			topic = new String(wordnet.getRandomWord("n"));
			topicSet.add(topic);
		}
		return new ArrayList<String>(topicSet);
	}
	
	/**
	 * Loads string lists into an array.
	 */
	
	public static ArrayList<String> load(final String path, final int number) throws IOException
	{
		// Return a list from a raw data file
		String element;
		ArrayList<String> array = new ArrayList<String>();
		BufferedReader buffer = new BufferedReader(new FileReader(path));		
		while (array.size() < number)
		{
			if ((element = buffer.readLine()) != null)
			{
				array.add(element);
			}
			else
			{
				throw new IOException("Null value in raw data file.");
			}
		}
		buffer.close();	
		return array;	
	}
	
	
}