import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {ConfigService} from "./config.service";
import {Router} from "@angular/router";
import {Observable} from "rxjs";

@Injectable({
  providedIn: 'root'
})
export class DeleteService {

  constructor(private readonly http: HttpClient, private readonly config: ConfigService, private readonly router: Router) {}


  deleteAccount(): Observable<any> {
    const url = `${this.config.api_url}/user`;
    return this.http.delete(url,  { responseType: 'text' });
  }


  deleteProject(id: string): Observable<any> {
    const url = `${this.config.project_api_url}/projects/${id}`;
    return this.http.delete(url,  { responseType: 'text' });
  }


}
