import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {ConfigService} from "./config.service";
import {Observable} from "rxjs";
import {Project} from "../models/project.model";
import {ProjectDetails} from "../models/projectDetails";

@Injectable({
  providedIn: 'root'
})
export class ProjectServiceService {

  constructor(private readonly http: HttpClient, private readonly config: ConfigService) { }

  addProject(newProjectRequest: Project): Observable<any> {
    return this.http.post<Project>(this.config.new_project_url, newProjectRequest)
  }

  deleteMemberFromProject(projectId: string, memberId: string): Observable<any> {
    const url = `${this.config.project_base_url}/projects/${projectId}/users/${memberId}`;
    return this.http.delete(url);
  }

  getProjectById(projectId: string): Observable<Project> {
    return this.http.get<Project>(this.config.getProjectByIdUrl(projectId))
  }

  getProjectDetailsById(projectId: string): Observable<ProjectDetails> {
    return this.http.get<ProjectDetails>(this.config.getProjectDetailsByIdUrl(projectId))
  }

  addMembersToProject(projectId: string, memberIds: string[]): Observable<any> {
    const url = `${this.config.addMembersUrl(projectId)}`;
    return this.http.post<string[]>(url, memberIds);
  }

  updateTaskStatus(taskId: string, status: string): Observable<void> {
    const body = { id: taskId, status: status };
    return this.http.put<void>(this.config.changeTaskStatus(), body, {
      headers: { 'Content-Type': 'application/json' }
    });
  }




}
