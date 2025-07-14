import {Component, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from "@angular/router";
import {AccountService} from "../services/account.service";

@Component({
  selector: 'app-account-verification',
  templateUrl: './account-verification.component.html',
  styleUrl: './account-verification.component.css'
})
export class AccountVerificationComponent implements OnInit{
  verificationStatus: 'success' | 'error' = 'success'; // This is set dynamically based on the backend response.

  constructor(private readonly router: Router,private readonly route: ActivatedRoute, private readonly service: AccountService) {}

  retryVerification() {
    this.router.navigate(['/register'])
  }

  ngOnInit(): void {
    const email = this.route.snapshot.paramMap.get('email');
    if (email) {
      this.verifyAccount(email);
    }
  }

  private verifyAccount(email: string) {
    this.service.verifyAccount(email).subscribe({
      next: () => {
        this.verificationStatus = 'success';
        setTimeout(() => {
          this.router.navigate(['/login']);
        }, 7000);
      },
      error: () => {
        this.verificationStatus = 'error';
      }
    })
  }
}
