package dn;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class CreateTable {

    public static void main(String[] args) {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_WEST_2;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

        String result = createTable(ddb, "dyno-test", "id");
        System.out.println("New table is " + result);
        ddb.close();
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
                        .readCapacityUnits(new Long(10))
                        .writeCapacityUnits(new Long(10))
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
