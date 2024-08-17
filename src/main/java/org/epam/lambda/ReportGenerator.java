package org.epam.lambda;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.LocalDate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ReportGenerator {
    private static final String DYNAMO_TABLE_NAME = "trainer_info";
    private static final String S3_BUCKET_NAME = "nikolozkiladze/reports";

    public static void main(String[] args) {
        AWSLambda awsLambda = AWSLambdaClientBuilder.defaultClient();

        generateAndUploadReport();
    }

    public static void generateAndUploadReport() {
        LocalDate now = LocalDate.now();

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.
                standard().withRegion(Regions.US_EAST_1).build();

        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        Table table = dynamoDB.getTable(DYNAMO_TABLE_NAME);

        Iterator<Item> it = table.scan().iterator();
        List<Item> items = new ArrayList<>();
        while (it.hasNext()) {
            items.add(it.next());
        }
        StringWriter writer = new StringWriter();
        try (CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Trainer First Name", "Trainer Last Name", "Current Month Trainings Duration"))) {
            for (Item item : items) {
                String firstName = item.getString("trainer_first_name");
                String lastName = item.getString("trainer_last_name");
                String status = item.getString("trainee_status").toLowerCase();
                int monthDuration = 0;
                List<Map<String, Object>> years = item.getList("years");

                for (Map<String, Object> year : years) {
                    String yearKey = year.keySet().iterator().next(); // Assuming each map has only one key, the year
                    if (yearKey.equals("" + now.getYear())) {
                        Map<String, Object> months = (Map<String, Object>) year.get(yearKey);
                        Number monthData = (Number) months.get("" + now.getMonthOfYear());
                        monthDuration = monthData.intValue();
                    }
                }
                if (!status.equals("inactive") && monthDuration > 0) {
                    csvPrinter.printRecord(firstName, lastName, monthDuration);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        AmazonS3 s3Client = AmazonS3ClientBuilder.
                standard().withRegion(Regions.US_EAST_1).build();
        byte[] contentAsBytes = writer.toString().getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(contentAsBytes);
        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(contentAsBytes.length);
        String reportName = "Trainers_Trainings_summary_" + now.getYear() + "_" + now.getMonthOfYear() + "_" + ".csv";

        s3Client.putObject(new PutObjectRequest(S3_BUCKET_NAME, reportName, contentsAsStream, md));
    }
}
