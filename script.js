require("dotenv").config();

const AWS = require("aws-sdk");
const { Storage } = require("@google-cloud/storage");

const awsAccessKeyId = process.env.AWS_ACCESS_KEY_ID;
const awsSecretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;

// Set your AWS credentials
AWS.config.update({
  accessKeyId: awsAccessKeyId,
  secretAccessKey: awsSecretAccessKey,
  region: "us-east-1",
  signatureVersion: "v4",
});

const projectId = process.env.PROJECT_ID;
const keyFilename = process.env.KEYFILE_PATH;

const s3 = new AWS.S3();

const storage = new Storage({ projectId, keyFilename });

// Function to fetch the list of AWS S3 buckets
async function listS3Buckets() {
  try {
    const response = await s3.listBuckets().promise();
    return response.Buckets.map((bucket) => bucket.Name);
  } catch (error) {
    console.error("Error listing S3 buckets:", error);
    return [];
  }
}

// Function to check if a GCS bucket exists
async function doesGCSBucketExist(bucketName) {
  try {
    const [exists] = await storage.bucket(bucketName).exists();
    console.log(`Bucket ${bucketName} exist.`);
    return exists;
  } catch (error) {
    console.error(
      `Error checking existence of GCS bucket ${bucketName}:`,
      error
    );
    return false;
  }
}

// Function to create a GCS bucket
async function createGCSBucket(bucketName) {
  try {
    await storage.createBucket(bucketName);
    console.log(`GCS bucket ${bucketName} created successfully.`);
  } catch (error) {
    console.error(`Error creating GCS bucket ${bucketName}:`, error);
  }
}

// Function to copy an object from S3 to GCS
async function copyObjectFromS3ToGCS(
  sourceBucketName,
  destinationBucketName,
  objectKey
) {
  const s3ObjectStream = s3
    .getObject({ Bucket: sourceBucketName, Key: objectKey })
    .createReadStream();
  const gcsObject = storage.bucket(destinationBucketName).file(objectKey);

  await new Promise((resolve, reject) => {
    s3ObjectStream
      .pipe(gcsObject.createWriteStream())
      .on("error", reject)
      .on("finish", resolve);
  });
}

// Function to check if an object exists in a GCS bucket
async function doesGCSObjectExist(bucketName, objectName) {
  try {
    const [exists] = await storage.bucket(bucketName).file(objectName).exists();
    return exists;
  } catch (error) {
    console.error(
      `Error checking existence of GCS object ${objectName} in bucket ${bucketName}:`,
      error
    );
    return false;
  }
}

// Function to transfer data from AWS S3 to GCS for a specific bucket in batches
async function transferFromS3ToGCS(
  sourceBucketName,
  destinationBucketName,
  batchSize
) {
  try {
    const gcsBucketExists = await doesGCSBucketExist(destinationBucketName);
    if (!gcsBucketExists) {
      await createGCSBucket(destinationBucketName);
    }

    let continuationToken;
    do {
      // List objects in the source S3 bucket with a batch size and continuation token
      const listObjectsResponse = await s3
        .listObjectsV2({
          Bucket: sourceBucketName,
          MaxKeys: batchSize,
          ContinuationToken: continuationToken,
        })
        .promise();

      const objects = listObjectsResponse.Contents;

      await Promise.all(
        objects.map(async (object) => {
          const gcsObjectExists = await doesGCSObjectExist(
            destinationBucketName,
            object.Key
          );
          if (!gcsObjectExists) {
            await copyObjectFromS3ToGCS(
              sourceBucketName,
              destinationBucketName,
              object.Key
            );
            console.log(`\n\nObject ${object.Key} copied successfully.`);
          } else {
            console.log(`Object ${object.Key} already exists in GCS bucket.`);
          }
        })
      );

      continuationToken = listObjectsResponse.NextContinuationToken;
    } while (continuationToken);

    console.log(
      `All objects from S3 bucket ${sourceBucketName} checked and copied to GCS bucket ${destinationBucketName}.`
    );
  } catch (error) {
    console.error(
      `Error transferring objects from S3 bucket ${sourceBucketName} to GCS bucket ${destinationBucketName}:`,
      error
    );
  }
}

async function runScript() {
  const awsS3Buckets = await listS3Buckets();

  for (const awsS3Bucket of awsS3Buckets) {
    await transferFromS3ToGCS(awsS3Bucket, awsS3Bucket, 1000);
  }
}

runScript();
