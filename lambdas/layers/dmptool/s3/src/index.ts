import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  S3Client,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  GetObjectCommand,
  GetObjectCommandOutput,
  PutObjectCommand,
  PutObjectCommandOutput,
} from "@aws-sdk/client-s3";

const s3Client = new S3Client({});

export interface DMPToolPresignedURLOutput {
  fileName: string;
  url: string;
}

// List the contents of the specified bucket that match the specified key prefix
export const listObjects = async (bucket: string, keyPrefix: string): Promise<ListObjectsV2CommandOutput> => {
  const listObjectsCommand = new ListObjectsV2Command({ Bucket: bucket, Prefix: keyPrefix });
  return await s3Client.send(listObjectsCommand);
}

// Fetch an object from the specified bucket
export const getObject = async (bucket: string, key: string): Promise<GetObjectCommandOutput> => {
  const command = new GetObjectCommand({ Bucket: bucket, Key: key });
  return await s3Client.send(command);
}

// Put an object into the specified bucket
export const putObject = async (bucket: string, key: string): Promise<PutObjectCommandOutput> => {
  const command = new PutObjectCommand({ Bucket: bucket, Key: key });
  return await s3Client.send(command);
}

// Generate a Pre-signed URL for an S3 object
export const getPresignedURL = async (
  bucket: string,
  key: string,
  usePutMethod = false
): Promise<DMPToolPresignedURLOutput> => {
  const params = { Bucket: bucket, Key: key };
  const command = usePutMethod ? new PutObjectCommand(params) : new GetObjectCommand(params);
  const presignedURL = await getSignedUrl(s3Client, command);

console.log(command)
console.log(params)
console.log(presignedURL)

  return { fileName: key, url: presignedURL };
}
