import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {ConfigService} from "./config.service";
import {Observable} from "rxjs";
import {Task} from "../models/task";

@Injectable({
  providedIn: 'root'
})
export class TaskService {

  constructor(private readonly http: HttpClient, private readonly config: ConfigService) { }

  getTasksByProjectId(projectId: string): Observable<Task[]> {
    return this.http.get<Task[]>(this.config.getTasksByProjectId(projectId)) }


  addTask(newTask: Task): Observable<{message: string, task: Task, taskId: string}> {
    return this.http.post<{ message: string, task: Task, taskId: string }>(`${this.config.new_task_url}/tasks`, newTask);
  }


  updateTaskStatus(task: Task) {
    const headers = { 'Content-Type': 'application/json' };
    return this.http.post(this.config.update_status_url, task, { headers });
  }

  checkIfUserInTask(task: Task): Observable<boolean> {
    const headers = { 'Content-Type': 'application/json' };
    return this.http.post<boolean>(this.config.check_task_url, task, { headers });
  }

  checkIfUserIsManager(projectId: string): Observable<boolean> {
    const headers = { 'Content-Type': 'application/json' };
    const url = `${this.config.checkManagerUrl(projectId)}`;
    return this.http.get<boolean>(url, { headers });
  }

  uploadTaskDocument(taskId: string, file: File): Observable<string> {
    const formData = new FormData();
    formData.append('taskId', taskId);
    formData.append('file', file);

    const url = `${this.config.uploadTaskDocumentUrl()}`;
    return this.http.post<any>(url, formData );
  }

  getAllDocumentsForThisTask(taskId: string): Observable<any> {
    const url = `${this.config.getForTaskAllDocumentUrl(taskId)}`;
    return this.http.get<any>(url);
  }



}
