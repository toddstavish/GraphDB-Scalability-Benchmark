package bench;


import org.neo4j.graphdb.RelationshipType;

/**
 * List of the static Neo4J relationship types.
 */

enum Neo4jRelationshipTypes implements RelationshipType
{
	TOPICS,
	TOPIC,
	GROUPS,
	GROUP,
	PEOPLE,
	PERSON,
	DOCUMENT,
	ASSOCIATED_TO,
	IS_MEMBER_OF,
	HAS,
	KNOWS,
	AUTHORS,
	VIEWS
}