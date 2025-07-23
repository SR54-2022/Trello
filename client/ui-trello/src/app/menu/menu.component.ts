import {Component, OnInit} from '@angular/core';
import {AccountService} from "../services/account.service";
import {ToastrService} from "ngx-toastr";
import {Router} from "@angular/router";
import {DeleteService} from "../services/delete.service";
import { CommonModule } from '@angular/common';

import {NotificationService} from "../services/notification-service.service";
import {FormToggleService} from "../form-toggle.service";

@Component({
  selector: 'app-menu',
  templateUrl: './menu.component.html',
  standalone: true,
  styleUrl: './menu.component.css',
  imports: [CommonModule]
})
export class MenuComponent implements OnInit {

  constructor(private readonly formToggleService: FormToggleService, private readonly accountService: AccountService, private readonly toastrService: ToastrService, private readonly router: Router, private readonly deleteService: DeleteService, private readonly notificationService: NotificationService) {

  }
  visible: boolean = false;
  userId: string | null = ""
  message: string = "Are you sure you want to delete your account?"
  unreadNotifications = 0;
  showForm: boolean = false;

  toggleForm(): void {
    this.formToggleService.toggleForm();
  }

  logout() {
    this.accountService.logout().subscribe({
      next: () => {
        this.toastrService.success("Logged out!");
        this.router.navigate(['/login'])
      },
      error: () => {
        this.toastrService.error('Logout failed');
    }
    })
  }

  deleteAccount() {
      this.deleteService.deleteAccount().subscribe({
        next: () => {
          this.accountService.logout().subscribe({
            next: () => {
              console.log("Account deleted and logged out successfully!");

              this.router.navigate(['/login']);
            },
            error: () => {
              this.toastrService.error("Account deleted, but logout failed.");
              console.log("Account deleted, but logout failed.");

            }
          });
          this.router.navigate(['/login']);

          this.toastrService.success("Successfully deleted profile.");

        },
        error: (error) => {
          console.log("Deleting account failed.");
          console.log("Response Body:", error.error);
          this.toastrService.error('Deleting account failed. ' + error.error);
        }
      })

    this.cancel();


  }
  cancel() {
    this.visible = !this.visible;
  }

  navigateToChangePassword() {
    this.router.navigate(['/changePassword']);
  }
  notifications(){
    this.router.navigate(['/notifications']);

  }

  fetchUnreadNotificationCount(): void {
    this.notificationService.getUnreadCount().subscribe({
      next: (response: { unreadCount: number }) => {
        this.unreadNotifications = response.unreadCount;
        console.log('Unread notifications count:', this.unreadNotifications);
      },
      error: (err) => {
        console.log('Error fetching notifications:', err);
      }
    });
  }

  ngOnInit(): void {
    this.fetchUnreadNotificationCount();

    this.formToggleService.showForm$.subscribe((state) => {
      this.showForm = state;
    });

  }

  isManager(): boolean {
    const role = localStorage.getItem('role');
    return role === 'manager';
  }

}
