import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {ConfigService} from "./config.service";
import {Router} from "@angular/router";
import {Observable} from "rxjs";
import {TaskGraph} from "../models/task-graph";

@Injectable({
  providedIn: 'root'
})
export class GraphService {

  constructor(private readonly http: HttpClient, private readonly config: ConfigService, private readonly router: Router) { }

  getWorkflowByProject(id: string): Observable<TaskGraph> {
    return this.http.get<TaskGraph>(this.config.getWorkflowByProject(id))
  }

  addDependency(taskID: string, dependencyID: string): Observable<any> {
      return this.http.post(this.config.postDependency(taskID, dependencyID), {})
  }
}
