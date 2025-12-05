// Test Auto-Trigger
const { S3Client, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { RekognitionClient, DetectLabelsCommand } = require("@aws-sdk/client-rekognition");
const { Client } = require("@opensearch-project/opensearch");

const s3 = new S3Client({});
const rek = new RekognitionClient({region: "us-east-1"});

//OpenSearch BASIC AUTH CLIENT
const osClient = new Client({
  node: process.env.OPENSEARCH_ENDPOINT,
  auth: {
    username: process.env.OPENSEARCH_USERNAME,
    password: process.env.OPENSEARCH_PASSWORD,
  }
});

// Auto-Create Index if Missing
async function ensurePhotosIndex() {
  const indexName = "photos";

  try {
    const exists = await osClient.indices.exists({ index: indexName });

    // exists.body === true means index exists
    if (exists.body === true) {
      console.log(`Index '${indexName}' exists`);
      return;
    }

    console.log(`Index '${indexName}' missing. Creating...`);

    const mapping = {
      mappings: {
        properties: {
          objectKey: { type: "keyword" },
          bucket: { type: "keyword" },
          labels: { type: "keyword" },
          createdTimestamp: { type: "date" }
        }
      }
    };

    await osClient.indices.create({
      index: indexName,
      body: mapping,
    });

    console.log(`Index '${indexName}' created`);
  } catch (err) {
    console.error("Error ensuring index:", err);
  }
}

// Lambda Handler
exports.handler = async (event) => {
  console.log("Received S3 event:", JSON.stringify(event));

  // Ensure index exists before inserting documents
  await ensurePhotosIndex();

  for (const record of event.Records || []) {
    const bucket = record.s3.bucket.name;
    const objectKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    try {
      console.log(`Processing: ${bucket}/${objectKey}`);

      // Extract S3 metadata (custom labels)
      const headObj = await s3.send(new HeadObjectCommand({
        Bucket: bucket,
        Key: objectKey,
      }));

      const metadata = headObj.Metadata || {};
      const customLabelStr =
        metadata.customlabels || metadata["custom-labels"] || "";

      const customLabels = customLabelStr
        ? customLabelStr.split(",").map(s => s.trim().toLowerCase()).filter(Boolean)
        : [];

      console.log("Custom Labels:", customLabels);

      // Rekognition labels
      const detectResp = await rek.send(new DetectLabelsCommand({
        Image: { S3Object: { Bucket: bucket, Name: objectKey } },
        MaxLabels: 50,
        MinConfidence: 60,
      }));

      const rekLabels =
        detectResp.Labels?.map(l => l.Name.toLowerCase()) || [];

      console.log("Rekognition Labels:", rekLabels);

      // Merge labels
      const labels = Array.from(new Set([...customLabels, ...rekLabels]));

      console.log("Final Labels:", labels);

      // Document for OpenSearch
      const doc = {
        objectKey,
        bucket,
        createdTimestamp: new Date().toISOString(),
        labels,
      };

      // Write document to OpenSearch
      const resp = await osClient.index({
        index: "photos",
        id: `${bucket}/${objectKey}`,
        body: doc,
        refresh: true,
      });

      console.log("Indexed successfully:", resp.body);

    } catch (err) {
      console.error("Error processing record:", err);
    }
  }

  return { statusCode: 200, body: "LF1 OK" };
};
