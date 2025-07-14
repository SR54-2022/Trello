import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {ConfigService} from "./config.service";
import {Observable} from "rxjs";

@Injectable({
  providedIn: 'root'
})
export class AnalyticsService {

  constructor(private readonly http: HttpClient, private readonly config: ConfigService) { }

  getEvents(projectID: string): Observable<any[]> {
    return this.http.get<any[]>(this.config.getHistoryByProjectId(projectID));
  }

  getAnalytics(projectID: string): Observable<any> {
    return this.http.get<any[]>(this.config.getAnalyticsByProjectId(projectID));
  }
}
