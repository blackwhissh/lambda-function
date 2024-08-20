package org.epam.lambda;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.logging.Logger;

public class ReportGenerator {
    private static final Logger LOGGER = Logger.getLogger("ReportGenerator");
    private static final String DYNAMO_TABLE_NAME = "trainer_info";
    private static final String S3_BUCKET_NAME = "nikolozkiladze/reports";
    private static final String CSV_FIRST_NAME = "Trainer First Name";
    private static final String CSV_LAST_NAME = "Trainer Last Name";
    private static final String CSV_DURATION = "Current Month Trainings Duration";
    private static final String FIRST_NAME = "trainer_first_name";
    private static final String LAST_NAME = "trainer_last_name";
    private static final String STATUS = "trainee_status";
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.
            standard().withRegion(Regions.US_EAST_1).build();

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

        IteratorSupport<Item, ScanOutcome> it = table.scan().iterator();
        List<Item> items = new ArrayList<>();
        while (it.hasNext()) {
            items.add(it.next());
        }
        StringWriter writer = new StringWriter();

        writeCSV(writer, items, now);

        saveInS3(writer, now, s3Client);
    }

    public static String generatePresignedUrl(String bucketName, String objectKey, int expirationInMinutes, AmazonS3 s3Client) {
        Date expiration = new Date();
        final long expTimeMillis = expiration.getTime() + 1000L * 60 * expirationInMinutes;
        expiration.setTime(expTimeMillis);

        GeneratePresignedUrlRequest generatePresignedUrlRequest =
                new GeneratePresignedUrlRequest(bucketName, objectKey)
                        .withMethod(com.amazonaws.HttpMethod.GET)
                        .withExpiration(expiration);
        URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

        return url.toString();
    }

    public static void saveInS3(StringWriter writer, LocalDate now, AmazonS3 amazonS3) {
        LOGGER.info("Started saving in S3");

        byte[] contentAsBytes = writer.toString().getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(contentAsBytes);
        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(contentAsBytes.length);
        String reportName = "Trainers_Trainings_summary_" + now.getYear() + "_" + now.getMonthValue() + "_" + ".csv";

        amazonS3.putObject(new PutObjectRequest(S3_BUCKET_NAME, reportName, contentsAsStream, md));

        String url = generatePresignedUrl(S3_BUCKET_NAME, reportName, 60, s3Client);
        LOGGER.info(url);
    }

    public static void writeCSV(StringWriter writer, List<Item> items, LocalDate now) {
        LOGGER.info("Started writing in CSV");
        try (CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(CSV_FIRST_NAME, CSV_LAST_NAME, CSV_DURATION))) {
            for (Item item : items) {
                String firstName = item.getString(FIRST_NAME);
                String lastName = item.getString(LAST_NAME);
                String status = item.getString(STATUS).toLowerCase();
                if (!status.equals("inactive")){
                    int monthDuration = 0;
                    List<Map<String, Object>> years = item.getList("years");

                    for (Map<String, Object> year : years) {
                        String yearKey = year.keySet().iterator().next();
                        if (yearKey.equals("" + now.getYear())) {
                            Map<String, Object> months = (Map<String, Object>) year.get(yearKey);
                            Number monthData = (Number) months.get("" + now.getMonth());
                            monthDuration = monthData.intValue();
                        }
                    }
                    if (monthDuration > 0) {
                        csvPrinter.printRecord(firstName, lastName, monthDuration);
                    }
                    LOGGER.info("CSV Created");
                }
            }
        } catch (IOException e) {
            LOGGER.warning("Error occurred: " + e.getMessage());
        }
    }
}
