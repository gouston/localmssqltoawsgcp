//snippet-sourcedescription:[PutObject.java demonstrates how to upload an object to an Amazon Simple Storage Service (Amazon S3) bucket.]
//snippet-keyword:[AWS SDK for Java v2]
//snippet-keyword:[Code Sample]
//snippet-service:[Amazon S3]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[01/07/2021]
//snippet-sourceauthor:[scmacdon-aws]

/*
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/
package com.birockstar.localmssqltoawsgcp.S3;

// snippet-start:[s3.java2.s3_object_upload.import]
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
// snippet-end:[s3.java2.s3_object_upload.import]

public class PutObject {

    static S3Client s3;
    static String bucketName;

    public PutObject() {
        bucketName = "rockstaretltesting";

        Region region = Region.US_EAST_1;
        s3 = S3Client.builder()
                .region(region)
                .build();

    }


    public static void main(String[] args) {
        final String USAGE = "\n" +
                "Usage:\n" +
                "  PutObject <bucketName> <objectKey> <objectPath> \n\n" +
                "Where:\n" +
                "  bucketName - the Amazon S3 bucket to upload an object into.\n" +
                "  objectKey - the object to upload (for example, book.pdf).\n" +
                "  objectPath - the path where the file is located (for example, C:/AWS/book2.pdf). \n\n" ;

        if (args.length != 3) {
            System.out.println(USAGE);
            System.exit(1);
        }

        String bucketName = args[0];
        String objectKey = args[1];
        String objectPath = args[2];

        System.out.println("Putting object " + objectKey +" into bucket " + bucketName);
        System.out.println("  in bucket: " + bucketName);

        /*Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();*/

        String result = new PutObject().putS3Object(/*s3, bucketName,*/ objectKey, objectPath);
        System.out.println("Tag information: "+result);
        s3.close();
    }

    // snippet-start:[s3.java2.s3_object_upload.main]
    public String putS3Object(/*S3Client s3,
                                     String bucketName,*/
                                     String objectKey,
                                     String objectPath) {

        try {

            Map<String, String> metadata = new HashMap<>();
            metadata.put("myVal", "test");

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            PutObjectResponse response = s3.putObject(putOb,
                    RequestBody.fromBytes(getObjectFile(objectPath)));

           return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    public String putS3Object(String objectKey, byte[] bytes) {

        try {

            Map<String, String> metadata = new HashMap<>();
            metadata.put("myVal", "test");

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            PutObjectResponse response = s3.putObject(putOb,
                    RequestBody.fromBytes(bytes));

           return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    // Return a byte array
    public static byte[] getObjectFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }
    // snippet-end:[s3.java2.s3_object_upload.main]
}