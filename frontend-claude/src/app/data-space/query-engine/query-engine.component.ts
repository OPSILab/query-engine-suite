import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { NbComponentStatus, NbGlobalLogicalPosition, NbGlobalPosition, NbToastrService } from '@nebular/theme';

import { BeopenAPIService } from '../../services/be-open.service';
import { BucketObject } from '../../model/BucketObject';
import { TranslateService } from '@ngx-translate/core';
import { FormGroup, FormControl } from '@angular/forms';
import { BeopenUser } from '../../model/beopen-user';
import { Router } from '@angular/router';
import { SharedService } from '../../services/shared.service';
import { DataSpaceService } from '../data-space.service';

/**
 * Ported from the main dashboard's QueryEngineComponent
 * (src/app/pages/data-space/query-engine/query-engine.component.ts).
 *
 * Changes made while porting:
 * - The "Query SQL" tab no longer loads an <iframe src="assets/autocomplete/index.html">
 *   talking back to this component through window.postMessage. It now embeds
 *   <bx-sql-editor> directly and binds its value with [(value)]="sqlQuery" -
 *   a real Angular component instead of a vanilla HTML/JS/CSS page copied into
 *   assets/ at build time (see angular.json in the main dashboard).
 * - sqlQuery is a plain instance field now. In the original file it was
 *   actually a module-level `export let sqlQuery` variable shadowing an
 *   unused `this.sqlQuery` class field of the same name (the postMessage
 *   handler wrote to the module-level one, minioQuery() read the same
 *   module-level one) - that indirection is gone along with the iframe.
 * - Fields that were declared but never read anywhere in this component
 *   (pdfSrc, isImage/isText/isPdf/isGeoJson, dialogData, map, ...) were
 *   dropped; they look like leftovers copy-pasted from DataSpaceComponent.
 * - configService is no longer injected: it was only used to build the old
 *   sqlEditorUrl.
 * - On a getUser() failure the original redirects to "/register-user" (a
 *   dashboard-only route). This standalone project has no such route, so it
 *   just surfaces a toast instead - adjust this if you wire the same auth
 *   flow back in.
 */
@Component({
  selector: 'ngx-query-engine',
  templateUrl: './query-engine.component.html',
  styleUrls: ['./query-engine.component.scss']
})
export class QueryEngineComponent implements OnInit {

  form: FormGroup;
  modes: string[] = ["Simple search", "Advanced search", "Query SQL"];
  lines: any[] = [{ key: "", value: "" }];
  type: string;
  value: string = "";
  sqlQuery: string = "";
  generalSharedBucketObjects: BucketObject[] = [];
  userBucketObjects: BucketObject[] = [];
  pilotSharedBucketObjects: BucketObject[] = [];
  extractedElements: any[] = [];
  isAdmin: boolean = true;
  beopenUser: BeopenUser;
  userRoles: string[] = [];
  types: string[] = ["CSV", "JSON", "GeoJSON"];
  valueTypes: string[] = ["String", "Date"];
  email: string | null = null;

  @Input() visibility!: string;
  @Output() extractedElementsChange = new EventEmitter<any[]>();
  @Output() generalSharedBucketObjectsChange = new EventEmitter<any[]>();
  @Output() pilotSharedBucketObjectsChange = new EventEmitter<any[]>();
  @Output() userBucketObjectsChange = new EventEmitter<any[]>();

  keys = [];
  values = [];
  entries: any[];
  valueVerified = [];
  keyVerified = [];

  constructor(
    private beopenAPI: BeopenAPIService,
    public translation: TranslateService,
    private toastrService: NbToastrService,
    private router: Router,
    private sharedService: SharedService,
    private dataSpaceService: DataSpaceService
  ) {
    this.form = new FormGroup({
      mode: new FormControl(this.modes[1])
    });
  }

  stringify(value) {
    if (typeof value == "string")
      return value;
    return JSON.stringify(value);
  }

  async ngOnInit(): Promise<void> {
    this.getUser();
    this.keys = Array.from(new Set((await this.beopenAPI.getKeys()).map(e => this.stringify(e.key))));
    this.values = Array.from(new Set((await this.beopenAPI.getValues()).map(e => this.stringify(e.value))));
    this.entries = await this.beopenAPI.getEntries();
  }

  getUser() {
    this.beopenAPI.getUser().subscribe(
      (beopenUser) => {
        this.beopenUser = beopenUser;
        this.email = beopenUser.email;
      },
      (err) => {
        console.warn("Could not load the current user", err);
        this.createToastr(NbGlobalLogicalPosition.BOTTOM_END, 'warning', "Not logged in", "Could not load the current user.");
      }
    );

    this.sharedService.userRoles$.subscribe((u) => {
      this.userRoles = u || [];
    });

    if (this.userRoles.includes("admin")) {
      this.isAdmin = true;
    }
  }

  get mode() {
    return this.form.get('mode').value;
  }

  demo() {
    this.lines = [
      {
        "key": "a",
        "value": "a1",
        "type": "String"
      }
    ];
    this.value = "a1";
    this.type = "JSON";
  }

  minioQuery() {
    let mongoQuery = {};
    for (let l of this.lines) {
      if (l.type == "Date")
        mongoQuery[l.key] = JSON.stringify({
          $gte: new Date(l.from),
          $lte: new Date(l.to),
        });
      else mongoQuery[l.key] = l.value;
    }
    this.beopenAPI.minioQuery(this.mode, this.value, mongoQuery, this.sqlQuery, this.visibility, this.type).subscribe(queryResult => {
      this.generalSharedBucketObjects = [];
      this.pilotSharedBucketObjects = [];
      this.userBucketObjects = [];
      this.extractedElements = [];
      for (let obj of queryResult) {
        obj.objectPath = obj.record.name;
        obj.pilot = obj.record.bucketName;
        obj.insertedBy = obj.record.insertedBy; //TODO now it is empty
        this.BucketObjectsPush(obj.record, this.isAdmin, this.generalSharedBucketObjects, this.pilotSharedBucketObjects, this.userBucketObjects, obj.record.pilot);
        this.extractedElements.push({ name: (obj.record.bucketName || obj.record.s3.bucket.name) + "/" + obj.name, element: obj.element });
      }
      this.sendData();
    }, err => {
      console.error("Query error", err);
      this.createToastr(NbGlobalLogicalPosition.BOTTOM_END, 'danger', "Error querying objects", err.error);
    });
  }

  BucketObjectsPush = this.dataSpaceService.BucketObjectsPush.bind(this.dataSpaceService);

  line(add) {
    if (add === 1) this.lines.push({ key: "", value: "" });
    else this.lines.pop();
  }

  createToastr(
    position: NbGlobalPosition,
    status: NbComponentStatus,
    message: string,
    description: string
  ) {
    try {
      return this.translation.get(description).subscribe((res: string) => {
        this.toastrService.show(res, message, {
          position,
          status,
          duration: 15000,
        });
      });
    } catch (error) {
      console.error(error);
      this.toastrService.show(description, message, {
        position,
        status,
        duration: 15000,
      });
    }
  }

  onKeysChange(data: any[], i) {
    this.lines[i].key = data;
  }

  valueVerifiedChange(data: any[], i) {
    this.valueVerified[i] = data;
  }

  keyVerifiedChange(data: any[], i) {
    this.keyVerified[i] = data;
  }

  onValuesChange(data: any[], i) {
    this.lines[i].value = data;
  }

  sendData() {
    this.extractedElementsChange.emit(this.extractedElements);
    this.generalSharedBucketObjectsChange.emit(this.generalSharedBucketObjects);
    this.pilotSharedBucketObjectsChange.emit(this.pilotSharedBucketObjects);
    this.userBucketObjectsChange.emit(this.userBucketObjects);
  }
}
