import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  S3Client,
  ListObjectsV2Command,
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

export interface DMPToolListObjectsOutput {
  key: string;
  lastModified: Date;
  size: number;
}

// List the contents of the specified bucket that match the specified key prefix
export const listObjects = async (bucket: string, keyPrefix: string): Promise<DMPToolListObjectsOutput[]> => {
  if (bucket && bucket.trim() !== '') {
    const listObjectsCommand = new ListObjectsV2Command({ Bucket: bucket, Prefix: keyPrefix });
    const response = await s3Client.send(listObjectsCommand);

    if (response && Array.isArray(response.Contents) && response.Contents.length > 0) {
      return response.Contents.map((entry) => {
        return { key: entry.Key, lastModified: entry.LastModified, size: entry.Size }
      });
    }
  }
  return undefined;
}

// Fetch an object from the specified bucket
export const getObject = async (bucket: string, key: string): Promise<GetObjectCommandOutput> => {
  if (bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    return await s3Client.send(command);
  }
  return undefined;
}

// Put an object into the specified bucket
export const putObject = async (bucket: string, key: string): Promise<PutObjectCommandOutput> => {
  if (bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
    return await s3Client.send(command);
  }
  return undefined;
}

// Generate a Pre-signed URL for an S3 object
export const getPresignedURL = async (
  bucket: string,
  key: string,
  usePutMethod = false
): Promise<DMPToolPresignedURLOutput> => {
  if (bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const params = { Bucket: bucket, Key: key };
    const command = usePutMethod ? new PutObjectCommand(params) : new GetObjectCommand(params);
    const presignedURL = await getSignedUrl(s3Client, command);

    return { fileName: key, url: presignedURL };
  }
  return undefined;
}
