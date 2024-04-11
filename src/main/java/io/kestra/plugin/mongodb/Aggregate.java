package io.kestra.plugin.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Run MongoDB Aggregate query."
)
@Plugin(
        examples = {
                @Example(
                        code = {
                                "connection:",
                                "  uri: \"mongodb://root:example@localhost:27017/?authSource=admin\"",
                                "database: \"my_database\"",
                                "collection: \"my_collection\"",
                                "compassQuery:",
                                " - $match:",
                                "    oid: 60930c39a982931c20ef6cd6",
                        }
                ),
        }
)
public class Aggregate extends AbstractTask implements RunnableTask<Aggregate.Output> {
    @Schema(
            title = "MongoDB Aggregate query.",
            description = "Can be a BSON string of the query."
    )
    @PluginProperty(dynamic = true)
    private List<Map<String, Object>> compassQuery;
    @Schema(
            title = "MongoDB Compass Aggregate Query",
            description = "BSON string with the aggregate query to execute."
    )

    @PluginProperty(dynamic = true)
    private Integer skip;

    @Schema(
            title = "Whether to store the data from the query result into an ion serialized data file."
    )
    @PluginProperty
    @Builder.Default
    private Boolean store = false;

    @Override
    public Aggregate.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<Document> collection = this.collection(runContext, client, Document.class);
            List<Bson> list = compassQuery
                    .stream()
                    .map(throwFunction(e -> MongoDbService.toBson(runContext, e)))
                    .toList();
            // Execute the aggregation pipeline
            AggregateIterable<Document> agg = collection.aggregate(list);
            Output.OutputBuilder builder = Output.builder();

            if (this.store) {
                Pair<URI, Long> store = this.store(runContext, agg);
                builder.uri(store.getLeft()).size(store.getRight());
            } else {
                Pair<ArrayList<Object>, Long> fetch = this.fetch(agg);
                builder.rows(fetch.getLeft()).size(fetch.getRight());
            }

            Output output = builder.build();

            // Metric tracking
            runContext.metric(Counter.of(
                    "records", output.getSize(),
                    "database", collection.getNamespace().getDatabaseName(),
                    "collection", collection.getNamespace().getCollectionName()
            ));

            return output;
        }
    }



    private Pair<URI, Long> store(RunContext runContext, AggregateIterable<Document> documents) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (OutputStream output = new FileOutputStream(tempFile)) {
            documents.forEach(throwConsumer(bsonDocument -> {
                count.incrementAndGet();
                FileSerde.write(output, MongoDbService.map(bsonDocument.toBsonDocument()));
            }));
        }

        return Pair.of(
                runContext.putTempFile(tempFile),
                count.get()
        );
    }

    private Pair<ArrayList<Object>, Long> fetch(AggregateIterable<Document> documents) {
        ArrayList<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents
                .forEach(throwConsumer(bsonDocument -> {
                    count.incrementAndGet();
                    result.add(MongoDbService.map(bsonDocument.toBsonDocument()));
                }));

        return Pair.of(
                result,
                count.get()
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "List containing the fetched data.",
                description = "Only populated if `store` parameter is set to false."
        )
        private List<Object> rows;

        @Schema(
                title = "The number of rows fetched."
        )
        private Long size;

        @Schema(
                title = "URI of the file containing the fetched results.",
                description = "Only populated if `store` parameter is set to true."
        )
        private URI uri;
    }
}
