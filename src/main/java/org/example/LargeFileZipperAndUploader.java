package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class LargeFileZipperAndUploader {

    private static final Logger logger = LoggerFactory.getLogger(LargeFileZipperAndUploader.class);

    public static void main(String[] args) {
        String largeFilePath = "C:/Users/320266356/BRITE/Projects/LargeFileZipperAndUploader/largefile_5GB.txt";
        String bucketName = "check-largezipfile";
        String keyName = "LargeFileKey/largefile_uploaded.zip";
        long maxMemory = Runtime.getRuntime().maxMemory();
        logger.info("Max memory: {} MB", maxMemory / (1024 * 1024));

        Region region = Region.US_EAST_1;
        try (S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build()) {
            zipAndUploadFileMultipart(s3Client, largeFilePath, bucketName, keyName);

        } catch (Exception e) {
            logger.error("Erro during the Upload of the file {}", e.getMessage());
        }
    }

    public static void zipAndUploadFileMultipart(S3Client s3Client, String largeFilePath, String bucketName, String keyName) {
        String uploadId = initiateMultipartUpload(s3Client, bucketName, keyName);

        try (
                PipedOutputStream pipedOutputStream = new PipedOutputStream();
             PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream, 1024 * 1024 * 4)) {
            AtomicBoolean isZippingComplete = new AtomicBoolean(false);
            Thread zippingThread = new Thread(() -> {
                try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(pipedOutputStream, 1024 * 1024 * 4))) {
                    zipOutputStream.putNextEntry(new ZipEntry(new File(largeFilePath).getName()));

                    byte[] buffer = new byte[1024 * 1024 * 4]; // 2 MB buffer size
                    try (InputStream fileInputStream = Files.newInputStream(Paths.get(largeFilePath))) {
                        int bytesRead;
                        while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                            zipOutputStream.write(buffer, 0, bytesRead);
                            zipOutputStream.flush();  // Flush
                            logger.debug("Zipping: wrote {} bytes to the zip output stream and flushed", bytesRead);
                        }
                    }

                    zipOutputStream.closeEntry();
                    zipOutputStream.finish();
                    logger.debug("Zipping complete, closing zip output stream.");
                } catch (IOException e) {
                    logger.error("Error during the zipping of the file  {}",e.getMessage());
                } finally {
                    isZippingComplete.set(true);
                }
            });

            zippingThread.start();

            List<CompletedPart> completedParts = new ArrayList<>();
            int partNumber = 1;
            ByteArrayOutputStream accumulatedBuffer = new ByteArrayOutputStream();
            byte[] partBuffer = new byte[1024 * 1024 * 4]; // 2 MB  buffer size for reading from PipedInputStream be careful with this buffer
            int bytesRead;

            while (true) {
                bytesRead = pipedInputStream.read(partBuffer);

                if (bytesRead > 0) {
                    accumulatedBuffer.write(partBuffer, 0, bytesRead);
                    logger.debug("Reading from piped input stream: read {} bytes, accumulated size: {}", bytesRead, accumulatedBuffer.size());

                    // 20 MB for each part we can change it if we want
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
                    break; // Exit
                }
            }

            zippingThread.join();

            completeMultipartUpload(s3Client, bucketName, keyName, completedParts, uploadId);
            logger.info("File uploaded successfully.");

        } catch (IOException | InterruptedException e) {
            logger.error("Error Uploading {}", e.getMessage());
        } catch (S3Exception e) {
            logger.error("Error S3 Exception  {}", e.getMessage());
        }
    }

    private static String initiateMultipartUpload(S3Client s3Client, String bucketName, String keyName) {
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();

        CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
        logger.info("Multipart upload initiated with Upload ID: {}", createMultipartUploadResponse.uploadId());
        return createMultipartUploadResponse.uploadId();
    }

    private static void completeMultipartUpload(S3Client s3Client, String bucketName, String keyName, List<CompletedPart> completedParts, String uploadId) {
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build();

        s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        logger.info("Multipart upload completed successfully for key: {}", keyName);
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
            logger.error("Failed to upload part number: {}", partNumber);
        }
    }
}
