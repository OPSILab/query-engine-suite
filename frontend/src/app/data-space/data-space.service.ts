import { Injectable } from '@angular/core';
import { DataSpaceFileEntity } from '../model/DataSpaceFileEntity';
import { BucketObject } from '../model/BucketObject';

@Injectable({
  providedIn: 'root'
})
export class DataSpaceService {

  constructor() { }

  BucketObjectsPush(element: DataSpaceFileEntity, isAdmin, generalSharedBucketObjects, pilotSharedBucketObjects, userBucketObjects, pilot) {
    console.debug("BucketObjectsPush", element);
    const sottostringhe = (element.objectPath || element.name).split("/");
    const ultimaSottostringa = sottostringhe[sottostringhe.length - 1];
    const estensioneIndex = ultimaSottostringa.indexOf(".");
    const estensione =
      estensioneIndex !== -1
        ? ultimaSottostringa.slice(estensioneIndex + 1)
        : "";

    const oggetto: BucketObject = {
      bucket: element.bucketName || sottostringhe[0],
      path: element.objectPath || element.name,
      etag: element.etag,
      fileName: sottostringhe[sottostringhe.length - 1],
      fileType: estensione,
      source: "Private",
      isGeoJsonUrban:
        estensione === "geojson" && (element.objectPath || element.name).includes("Urban"),
      isDeletable: isAdmin,
      insertedBy: element.insertedBy,
      lastModified: element.lastModified,
      pilot: element.pilot || element.bucketName,
      size: element.size,
    };
    if (oggetto.path.includes("public-data")) {
      oggetto.source = "General-Share";
      generalSharedBucketObjects.push(oggetto);
    } else if (oggetto.path.includes(pilot + " SHARED")) {
      oggetto.source = "Pilot-Share";
      pilotSharedBucketObjects.push(oggetto);
    } else {
      if (oggetto.fileName != oggetto.bucket) {
        oggetto.isDeletable = true;
        userBucketObjects.push(oggetto);
      }
    }
  }

}
