import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {ConfigService} from "./config.service";
import {Router} from "@angular/router";
import {Observable} from "rxjs";
import {Notification} from "../notifications/notifications.component";

@Injectable({
  providedIn: 'root'
})
export class NotificationService {

  constructor(private readonly http: HttpClient,
              private readonly config: ConfigService,
              private readonly router: Router) { }

  getNotificationById(id: string): Observable<Notification> {
    return this.http.get<Notification>(this.config.notifications_url);
  }

  updateNotificationStatus(id: string, status: string, createdAt: string): Observable<void> {
    const body = {
      status: status,
      created_at: createdAt,
    };
    return this.http.put<void>(this.config.notifications_url + `/` + id, body);
  }

  getNotificationByUserId(): Observable<Notification[]> {
    return this.http.get<Notification[]>(this.config.notifications_url + '/user/' );
  }

  getAllNotifications(): Observable<Notification[]> {
    return this.http.get<Notification[]>(this.config.notifications_url );

  }

  getUnreadCount(): Observable<{ unreadCount: number }> {
    return this.http.get<{ unreadCount: number }>(this.config.notifications_url + '/unread-count');
  }
}
