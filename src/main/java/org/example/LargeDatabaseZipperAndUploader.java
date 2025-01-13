package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class LargeDatabaseZipperAndUploader {

    private static final Logger logger = LoggerFactory.getLogger(LargeDatabaseZipperAndUploader.class);

    public static void main(String[] args) {
        String bucketName = "check-largezipfile";
        String keyName = "LargeDatabaseData/uploaded_data.zip";
        long maxMemory = Runtime.getRuntime().maxMemory();
        logger.info("Max memory: {} MB", maxMemory / (1024 * 1024));

        Region region = Region.US_EAST_1;
        try (S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build()) {
            zipAndUploadDatabaseDataMultipart(s3Client, bucketName, keyName);
        } catch (Exception e) {
            logger.error("Error during the Upload of the data {}", e.getMessage());
        }
    }

    public static void zipAndUploadDatabaseDataMultipart(S3Client s3Client, String bucketName, String keyName) {
        String uploadId = initiateMultipartUpload(s3Client, bucketName, keyName);

        try (
                PipedOutputStream pipedOutputStream = new PipedOutputStream();
                PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream, 1024 * 1024 * 4)) {
            AtomicBoolean isZippingComplete = new AtomicBoolean(false);
            Thread zippingThread = new Thread(() -> {
                try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(pipedOutputStream, 1024 * 1024 * 4))) {
                    // Example for one table; repeat for multiple tables if needed
                    zipOutputStream.putNextEntry(new ZipEntry("table_data.csv"));

                    String query = "SELECT * FROM your_large_table"; // Adjust your query
                    try (DatabaseStreamer dbStreamer = new DatabaseStreamer("jdbc:your_db_url", "user", "password");
                         ResultSet rs = dbStreamer.streamData(query)) {

                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        StringBuilder header = new StringBuilder();
                        for (int i = 1; i <= columnCount; i++) {
                            header.append(metaData.getColumnName(i));
                            if (i < columnCount) header.append(',');
                        }
                        header.append('\n');
                        zipOutputStream.write(header.toString().getBytes(StandardCharsets.UTF_8));

                        // Stream data row by row
                        StringBuilder rowBuilder = new StringBuilder();
                        while (rs.next()) {
                            rowBuilder.setLength(0);
                            for (int i = 1; i <= columnCount; i++) {
                                String data = rs.getString(i);
                                rowBuilder.append(data != null ? escapeCsv(data) : "NULL");
                                if (i < columnCount) rowBuilder.append(',');
                            }
                            rowBuilder.append('\n');
                            zipOutputStream.write(rowBuilder.toString().getBytes(StandardCharsets.UTF_8));

                            // Flush periodically to send data to piped stream
                            if (rowBuilder.length() > 1024 * 10) { // Example condition
                                zipOutputStream.flush();
                            }
                        }
                    }

                    zipOutputStream.closeEntry();
                    zipOutputStream.finish();
                    logger.debug("Zipping complete, closing zip output stream.");
                } catch (IOException | SQLException e) {
                    logger.error("Error during the zipping of the database data: {}", e.getMessage());
                } finally {
                    isZippingComplete.set(true);
                }
            });

            zippingThread.start();

            List<CompletedPart> completedParts = new ArrayList<>();
            int partNumber = 1;
            ByteArrayOutputStream accumulatedBuffer = new ByteArrayOutputStream();
            byte[] partBuffer = new byte[1024 * 1024 * 4]; // 4 MB buffer size
            int bytesRead;

            while (true) {
                bytesRead = pipedInputStream.read(partBuffer);

                if (bytesRead > 0) {
                    accumulatedBuffer.write(partBuffer, 0, bytesRead);
                    logger.debug("Reading from piped input stream: read {} bytes, accumulated size: {}", bytesRead, accumulatedBuffer.size());

                    // 20 MB for each part; adjust as needed
                    if (accumulatedBuffer.size() >= 20 * 1024 * 1024) {
                        byte[] dataToUpload = accumulatedBuffer.toByteArray();
                        uploadPart(s3Client, bucketName, keyName, uploadId, partNumber, dataToUpload, completedParts);
                        partNumber++;
                        accumulatedBuffer.reset();
                    }
                } else if (bytesRead == -1 && isZippingComplete.get()) {
                    if (accumulatedBuffer.size() > 0) {
                        byte[] dataToUpload = accumulatedBuffer.toByteArray();
                        uploadPart(s3Client, bucketName, keyName, uploadId, partNumber, dataToUpload, completedParts);
                        logger.info("Uploaded last part: part number : {}", partNumber);
                        partNumber++;
                    }
                    break; // Exit loop
                }
            }

            zippingThread.join();

            completeMultipartUpload(s3Client, bucketName, keyName, completedParts, uploadId);
            logger.info("Database data uploaded successfully.");

        } catch (IOException | InterruptedException e) {
            logger.error("Error Uploading {}", e.getMessage());
            abortMultipartUpload(s3Client, bucketName, keyName, uploadId);
        } catch (S3Exception e) {
            logger.error("S3 Exception: {}", e.getMessage());
            abortMultipartUpload(s3Client, bucketName, keyName, uploadId);
        }
    }

    private static String initiateMultipartUpload(S3Client s3Client, String bucketName, String keyName) {
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
        logger.info("Multipart upload initiated with Upload ID: {}", response.uploadId());
        return response.uploadId();
    }

    private static void completeMultipartUpload(S3Client s3Client, String bucketName, String keyName, List<CompletedPart> completedParts, String uploadId) {
        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build();

        s3Client.completeMultipartUpload(completeRequest);
        logger.info("Multipart upload completed successfully for key: {}", keyName);
    }

    private static void abortMultipartUpload(S3Client s3Client, String bucketName, String keyName, String uploadId) {
        AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .uploadId(uploadId)
                .build();

        s3Client.abortMultipartUpload(abortRequest);
        logger.info("Multipart upload aborted for key: {}", keyName);
    }

    private static void uploadPart(S3Client s3Client, String bucketName, String keyName, String uploadId, int partNumber, byte[] dataToUpload, List<CompletedPart> completedParts) {
        try {
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength((long) dataToUpload.length)
                    .build();

            UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(dataToUpload));
            completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadPartResponse.eTag())
                    .build());

            logger.info("Uploaded part {}, size: {} bytes", partNumber, dataToUpload.length);
        } catch (S3Exception e) {
            logger.error("Failed to upload part number: {}. Error: {}", partNumber, e.getMessage());
            throw e; // Re-throw to trigger abort in the calling method
        }
    }

    private static String escapeCsv(String field) {
        if (field.contains(",") || field.contains("\"") || field.contains("\n")) {
            field = field.replace("\"", "\"\"");
            return "\"" + field + "\"";
        }
        return field;
    }
}
