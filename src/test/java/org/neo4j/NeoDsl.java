package org.neo4j;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.lang.reflect.Field;

import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Stream.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 13.01.15
 */
public class NeoDsl {


    private ExecutionEngine cypher;
    private GraphDatabaseService gds;
    private Transaction tx;

    interface Token {
        String name();

        int ordinal();
    }

    interface Label extends org.neo4j.graphdb.Label, Token {
    }

    interface RelType extends RelationshipType, Token {
    }

    interface PropertyName extends Token {
    }

    interface EntityStream<T extends Entity> {
        Stream<T> stream();
    }

    interface MatchEntityStream  extends  EntityStream<Node> {
        EntityStream<Node> property(PropertyName property, Object value);
    }


    interface Match<T extends Entity> extends EntityStream<T> {
        EntityStream<Node> match(long... ids);

        EntityStream<Node> match(Label... labels);

//        Match property(PropertyName property, Object value);

//        Match where(Predicate<Node> predicate);
//
//        Match where(Predicate<Relationship> predicate);

    }


    interface Property<T> {

    }

    interface Entity {
        <T> T property(PropertyName prop);

        <T> T property(Token prop, Class<? extends T> type);

//        <T> Optional<T> property(PropertyName prop);

//        Property<T> property(PropertyName prop);
    }

    interface Relationship extends Entity {
        Node start();

        Node end();

        Token type();
    }

    static class Node implements Entity {
        private final ThreadToStatementContextBridge ctx;
        private final long id;

        public Node(ThreadToStatementContextBridge ctx, long id) {
            this.ctx = ctx;
            this.id = id;
        }

        Stream<Relationship> rels(Direction direction, RelType... type) {
            return null;
        }

        Stream<Relationship> rels(RelType... type) {
            return null;

        }

        Stream<Relationship> rels() {
            return null;

        }

        @Override
        public <T> T property(PropertyName prop) {
            return null;
        }

        @Override
        public <T> T property(Token prop, Class<? extends T> type) {
            return null;
        }

        public boolean hasLabel(Label label) {
            try {
                return ctx.instance().readOperations().nodeHasLabel(id, label.ordinal());
            } catch (EntityNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class Database implements Match<Node> {
        GraphDatabaseAPI db;
        private final ThreadToStatementContextBridge ctx;
//        private final ReadOperations readOps;

        private static Field ordinalField;

        static {
            try {
                ordinalField = Enum.class.getDeclaredField("ordinal");
                ordinalField.setAccessible(true);
            } catch (ReflectiveOperationException roe) {
                throw new RuntimeException(roe);
            }
        }

        public <T extends Enum & Token> Database(GraphDatabaseService db, Class<? extends T>... tokens) {
            this.db = (GraphDatabaseAPI) db;
            ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            of(tokens).forEach((token) -> of(token.getEnumConstants()).forEach(this::setOrdinal));
        }

        private <T extends Enum & Token> void setOrdinal(T token) {
            try (Transaction tx = db.beginTx()) {
                final TokenWriteOperations tokenWriteOps = null; // ctx.instance().tokenWriteOperations();
                ordinalField.set(token, idForToken(tokenWriteOps, token));
            } catch (IllegalTokenNameException | TooManyLabelsException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        private <T extends Enum & Token> int idForToken(TokenWriteOperations tokenWriteOps, T t) throws IllegalTokenNameException, TooManyLabelsException {
            String name = t.name();
            if (t instanceof Label) return tokenWriteOps.labelGetOrCreateForName(name);
            if (t instanceof RelType) return tokenWriteOps.relationshipTypeGetOrCreateForName(name);
            if (t instanceof PropertyName) return tokenWriteOps.propertyKeyGetOrCreateForName(name);
            throw new RuntimeException("Invalid token type " + t.getClass());
        }

        public void shutdown() {
            db.shutdown();
        }

        @Override
        public EntityStream<Node> match(long... ids) {
            return () -> toNodeStream(LongStream.of(ids));
        }

        private Stream<Node> toNodeStream(LongStream idStream) {
            return idStream.mapToObj(this::toNode);
        }

        @Override
        public MatchEntityStream match(Label... labels) {
            ReadOperations readOps = ctx.instance().readOperations();
            return new MatchEntityStream() {
                @Override
                public EntityStream<Node> property(PropertyName property, Object value) {
                    return null;
                }

                @Override
                public Stream<Node> stream() {
                    return of(labels)
                            .map(label -> readOps.nodesGetForLabel(label.ordinal()))
                            .map(Database::toStream)
                            .flatMap((longStream) -> toNodeStream(longStream));
                }
            };

        }

        private Node toNode(long id) {
            return new Node(ctx, id);
        }


        @Override
        public Stream<Node> stream() {
            //TODO look into cypher for better op
            return StreamSupport.stream(GlobalGraphOperations.at(db).getAllNodes().spliterator(), false)
                    .map(n -> toNode(n.getId()));
        }

        private static LongStream toStream(final PrimitiveLongIterator iterator) {
            return StreamSupport.longStream(Spliterators.spliteratorUnknownSize(new PrimitiveIterator.OfLong() {

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public long nextLong() {
                    return iterator.next();
                }
            }, Spliterator.ORDERED), false);
        }

    }


    enum Labels implements Label {
        Person, Company;
    }

    enum Rels implements RelType {
        KNOWS;
    }

    enum Properties implements PropertyName {
        name;
    }

    Database db;

//    {
//        IndexDescriptor descriptor = ops.indexesGetForLabelAndPropertyKey(ops.labelGetForName(Labels.LABEL.name()), ops.propertyKeyGetForName(PROPERTY_KEY));
//        PrimitiveLongIterator it = ops.nodesGetFromIndexLookup(descriptor, "bar" + i);
//
//        Iterator<DefinedProperty> props = ops.nodeGetAllProperties(node);
//        Property prop = ops.nodeGetProperty(node, propertyId);
//
//
//        db.match(labels).property(property, value).where((m) -> m.property(prop).equals("Thomas"))
//    }

    @Before
    public void setUp() throws Exception {
        gds = new TestGraphDatabaseFactory().newImpermanentDatabase();
        cypher = new ExecutionEngine(gds);
        cypher.execute("CYPHER 2.1 CREATE (n:A:B:C {dummy:0, dummy2:1})-[r:DUMMY]->(n) DELETE n,r");
        db = new Database(gds, Labels.class, Rels.class, Properties.class);
        tx = gds.beginTx();
    }

    @After
    public void tearDown() throws Exception {
        tx.failure();
        tx.close();
        db.shutdown();
    }

    @Test
    public void testInitializeTokens() throws Exception {
        assertEquals(3, Labels.Person.ordinal());
        assertEquals(2, Properties.name.ordinal());
        assertEquals(1, Rels.KNOWS.ordinal());
    }

    @Test
    public void testMatchNodeByLabel() throws Exception {
        ExecutionResult execute = cypher.execute("CYPHER 2.1 CREATE (n:Person),(m) return id(n)");
        long nodeId = IteratorUtil.single(execute.columnAs("id(n)"));
        assertEquals(1, db.match(Labels.Person).stream().count());
        assertEquals(nodeId, db.match(Labels.Person).stream().findFirst().get().id);
    }
    @Test
    public void testMatchNodeByLabelAndProperty() throws Exception {
        ExecutionResult execute = cypher.execute("CYPHER 2.1 CREATE (n:Person),(m:Person {name:'Michael') return id(m)");
        long nodeId = IteratorUtil.single(execute.columnAs("id(,)"));
        assertEquals(1, db.match(Labels.Person).property(Properties.name,"Michael").stream().count());
        assertEquals(nodeId, db.match(Labels.Person).stream().findFirst().get().id);
    }

    @Test
    public void testMatchNodeById() throws Exception {
        ExecutionResult execute = cypher.execute("CYPHER 2.1 CREATE (n),(m) return id(n)");
        long nodeId = IteratorUtil.single(execute.columnAs("id(n)"));
        assertEquals(1, db.match(nodeId).stream().count());
        assertEquals(nodeId, db.match(nodeId).stream().findFirst().get().id);
    }


    //filter((n) -> n.hasLabel(Labels.Person) || n.hasLabel(Labels.Company))
    @Test
    public void testMatch() throws Exception {
        cypher.execute("CYPHER 2.1 CREATE (n),(m)");

        Stream<Node> stream = db.stream();
        assertEquals(2, stream.count());
    }
//
//    @Test
//    public void testMatchByLabel() throws Exception {
//        db.match(Labels.Person).stream().count();
//    }
}
