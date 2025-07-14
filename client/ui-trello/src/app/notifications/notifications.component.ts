import {Component, OnInit} from '@angular/core';
import {NotificationService} from "../services/notification-service.service";
import {AccountService} from "../services/account.service";

export interface Notification {
  id: string;
  user_id: string;
  message: string;
  created_at: string;
  status: 'unread' | 'read';
}

@Component({
  selector: 'app-notifications',
  templateUrl: './notifications.component.html',
  styleUrl: './notifications.component.css'
})
export class NotificationsComponent implements OnInit{
  notifications: Notification[] = [];
  errorMessage: string = '';

  constructor(private readonly notificationService: NotificationService, private readonly accService: AccountService) {}

  ngOnInit(): void {
    this.loadNotifications();
  }

  loadNotifications(): void {
    this.notificationService.getAllNotifications().subscribe({
      next: (notifications) => {
        this.notifications = notifications;
        console.log(this.notifications);
      },
      error: (err) => {
        this.errorMessage = 'Failed to load notifications';
        console.error(err);
      },
    });
  }

  toggleStatus(notification: Notification): void {
    const newStatus = notification.status === 'read' ? 'unread' : 'read';
    this.notificationService.updateNotificationStatus(notification.id, newStatus, notification.created_at).subscribe({
      next: () => {
        notification.status = newStatus;
      },
      error: (err) => {
        this.errorMessage = 'Failed to update notification status';
        console.error(err);
      },
    });
  }
}
