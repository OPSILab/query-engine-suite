export interface BucketObject {
  bucket: string;
  fileName: string;
  path: string;
  etag: string;
  fileType: string;
  source: string;
  isGeoJsonUrban: boolean;
  isDeletable: boolean;
  insertedBy: string;
  lastModified: string;
  pilot: string;
  size: number;
}
