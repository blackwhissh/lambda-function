package org.epam.lambda;


import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.LocalDate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

public class ReportGenerator {
    private static final Logger LOGGER = Logger.getLogger("ReportGenerator");
    private static final String DYNAMO_TABLE_NAME = "trainer_info";
    private static final String S3_BUCKET_NAME = "nikolozkiladze/reports";

    public static void main(String[] args) {
        AWSLambda awsLambda = AWSLambdaClientBuilder.defaultClient();

        generateAndUploadReport();
    }

    public static void generateAndUploadReport() {
        LocalDate now = LocalDate.now();
        LOGGER.info("Lambda is executed at " + now);
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

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
                    String yearKey = year.keySet().iterator().next();
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

        String url = generatePresignedUrl(S3_BUCKET_NAME, reportName, 60, s3Client);
        LOGGER.info(url);
    }

    public static String generatePresignedUrl(String bucketName, String objectKey, int expirationInMinutes, AmazonS3 s3Client) {
        Date expiration = new Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000L * 60 * expirationInMinutes;
        expiration.setTime(expTimeMillis);

        GeneratePresignedUrlRequest generatePresignedUrlRequest =
                new GeneratePresignedUrlRequest(bucketName, objectKey)
                        .withMethod(com.amazonaws.HttpMethod.GET)
                        .withExpiration(expiration);
        URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

        return url.toString();
    }
}
