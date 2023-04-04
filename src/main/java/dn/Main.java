package dn;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;

public class Main {

    public static void main(String[] args) throws IOException {
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_WEST_2;

        DynamoDbClient dynamoDbClient = DynamoDbClient
                .builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

        String tableName = "dyno-test";

//        updateDynamoDBTable(dynamoDbClient, tableName, 25L, 25L);
//        putItemInTable(dynamoDbClient, tableName, "id",
//                "3",
//                "albumTitle",
//                "albumTitleValue",
//                "awards",
//                "awardVal",
//                "songTitle",
//                "songTitleVal",
//                "Nepal",
//                "Rock"
//        );

//        getDynamoDBItem(dynamoDbClient, tableName, "id", "2");
//        putItemInTable(dynamoDbClient, tableName, "id", "2", "songTitle",
//                "",
//                "",
//                "",
//                "",
//                "",
//                "",
//                Files.readString(Paths.get("/home/shree/Downloads/largeentity.json")));
        DynamoDbEnhancedClient dynamoDbEnhancedClient = DynamoDbEnhancedClient
                .builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
        putBatchRecords(dynamoDbEnhancedClient);

    }


    static void updateDynamoDBTable(DynamoDbClient dynamoDbClient, String tableName, Long readCapacity,
                                    Long writeCapacity) {
        System.out.printf("Updating %s with new provisioned throughput values \n", tableName);
        System.out.printf("Read capacity : %d\n", readCapacity);
        System.out.printf("Write capacity: %d\n", writeCapacity);

        ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacity)
                .writeCapacityUnits(writeCapacity)
                .build();

        UpdateTableRequest request = UpdateTableRequest.builder()
                .provisionedThroughput(provisionedThroughput)
                .tableName(tableName)
                .build();

        dynamoDbClient.updateTable(request);
    }

    static void putItemInTable(DynamoDbClient dynamoDbClient,
                               String tableName,
                               String key,
                               String keyVal,
                               String albumTitle,
                               String albumTitleValue,
                               String awards,
                               String awardVal,
                               String songTitle,
                               String songTitleVal,
                               String country,
                               String genre
    ) {
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(key, AttributeValue.builder().s(keyVal).build());
        itemValues.put(songTitle, AttributeValue.builder().s(songTitleVal).build());
        itemValues.put(albumTitle, AttributeValue.builder().s(albumTitleValue).build());
        itemValues.put(awards, AttributeValue.builder().s(awardVal).build());
        itemValues.put("country", AttributeValue.builder().s(country).build());
        itemValues.put("genre", AttributeValue.builder().s(genre).build());

        PutItemRequest request = PutItemRequest.builder().tableName(tableName)
                .item(itemValues)
                .build();


        TableWriteItems threadTableWriteItems = new TableWriteItems(tableName)
                .withItemsToPut(new Item().withPrimaryKey("ForumName", "Amazon RDS", "Subject", "Amazon RDS Thread 1")
                        .withString("Message", "ElastiCache Thread 1 message")
                        .withStringSet("Tags", new HashSet<String>(Arrays.asList("cache", "in-memory"))))
                .withHashAndRangeKeysToDelete("ForumName", "Subject", "Amazon S3", "S3 Thread 100");
        PutItemResponse response = dynamoDbClient.putItem(request);

        dynamoDbClient.batchWriteItem((Consumer<BatchWriteItemRequest.Builder>) threadTableWriteItems);

        System.out.println(tableName + " was successfully update. The request id is " + response.responseMetadata().requestId());
    }

    static void getDynamoDBItem(DynamoDbClient dynamoDbClient, String tableName, String key, String keyVal) {
        HashMap<String, AttributeValue> keyToGet = new HashMap<>();
        keyToGet.put(key, AttributeValue.builder().s(keyVal).build());

        GetItemRequest request = GetItemRequest.builder()
                .key(keyToGet)
                .tableName(tableName)
                .build();

        Map<String, AttributeValue> returnedItem = dynamoDbClient.getItem(request).item();
        if (returnedItem != null) {
            Set<String> keys = returnedItem.keySet();
            System.out.println("Amazon DynamoDB table attributes: \n");

            for (String key1 : keys) {
                System.out.printf("%s: %s \n", key1, returnedItem.get(key1).toString());
            }
        }
    }

    static void updateTableItem(DynamoDbClient dynamoDbClient,
                                String tableName,
                                String key,
                                String keyVal,
                                String name,
                                String updateVal) {
        HashMap<String, AttributeValue> itemKey = new HashMap<>();
        itemKey.put(key, AttributeValue.builder().s(keyVal).build());

        HashMap<String, AttributeValueUpdate> updatedValues = new HashMap<>();
        updatedValues.put(name, AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s(updateVal).build())
                .action(AttributeAction.PUT)
                .build());

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(itemKey)
                .attributeUpdates(updatedValues)
                .build();

        dynamoDbClient.updateItem(request);


    }

    static void putBatchRecords(DynamoDbEnhancedClient enhancedClient) throws IOException {

        DynamoDbTable<Customer> customerMappedTable = enhancedClient.table("Customer", TableSchema.fromBean(Customer.class));

        LocalDate localDate = LocalDate.parse("2020-04-07");
        LocalDateTime localDateTime = localDate.atStartOfDay();
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

        Customer record2 = new Customer();
        record2.setName("Fred Pink");
        record2.setId("id110");
        record2.setEmail("fredp@noserver.com");
        record2.setRegDate(instant);

        Customer record3 = new Customer();
        record3.setName("Susan Pink");
        record3.setId("id120");
        record3.setEmail("spink@noserver.com");
        record3.setRegDate(instant);

        Customer record4 = new Customer();
        record4.setName("Jerry orange");
        record4.setId("id101");
        record4.setEmail(Files.readString(Paths.get("/home/shree/Downloads/largeentity.json")));
        record4.setRegDate(instant);


        BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
                .writeBatches(WriteBatch.builder(Customer.class)
                        .mappedTableResource(customerMappedTable)
                        .addPutItem(customerBuilder -> customerBuilder.item(record2))
                        .addPutItem(customerBuilder -> customerBuilder.item(record3))
                        .addPutItem(customerBuilder -> customerBuilder.item(record4))
                        .build())
                .build();

        enhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
    }

    public static String createTable(DynamoDbClient dynamoDbClient, String tableName, String key) {
        DynamoDbWaiter dynamoDbWaiter = dynamoDbClient.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(key)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(key)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(new Long(25))
                        .writeCapacityUnits(new Long(25))
                        .build())
                .tableName(tableName)
                .build();

        String newTable = "";

        CreateTableResponse response = dynamoDbClient.createTable(request);
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
        WaiterResponse<DescribeTableResponse> waiterResponse = dynamoDbWaiter.waitUntilTableExists(describeTableRequest);
        waiterResponse.matched().response().ifPresent(System.out::println);
        newTable = response.tableDescription().tableName();
        return newTable;
    }
}
