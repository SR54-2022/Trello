import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { AccountService } from '../services/account.service';
import { ToastrService } from 'ngx-toastr';

@Component({
  selector: 'app-password-reset',
  templateUrl: './password-reset.component.html',
  styleUrls: ['./password-reset.component.css'],
})
export class PasswordResetComponent implements OnInit {
  newPassword: string = '';
  confirmPassword: string = '';
  email: string = '';

  passwordValidation = {
    length: false,
    uppercase: false,
    lowercase: false,
    number: false,
    specialChar: false,
  };

  constructor(
    private readonly route: ActivatedRoute,
    private readonly accountService: AccountService,
    private readonly toastr: ToastrService,
    private readonly router: Router
  ) {}

  ngOnInit(): void {
    this.email = this.route.snapshot.paramMap.get('email') ?? '';
  }

  validatePassword() {
    const password = this.newPassword;
    this.passwordValidation.length = password.length >= 8;
    this.passwordValidation.uppercase = /[A-Z]/.test(password);
    this.passwordValidation.lowercase = /[a-z]/.test(password);
    this.passwordValidation.number = /\d/.test(password);
    this.passwordValidation.specialChar = /[!@#$%^&*]/.test(password);
  }

  isPasswordValid(): boolean {
    return (
      this.passwordValidation.length &&
      this.passwordValidation.uppercase &&
      this.passwordValidation.lowercase &&
      this.passwordValidation.number &&
      this.passwordValidation.specialChar &&
      this.newPassword === this.confirmPassword
    );
  }

  resetPassword() {
    if (!this.isPasswordValid()) {
      this.toastr.error('Please meet all password requirements.');
      return;
    }

    if (this.newPassword !== this.confirmPassword) {
      this.toastr.error('Passwords do not match');
      return;
    }


    this.accountService.resetPassword(this.email, this.newPassword).subscribe({
      next: () => {
        this.toastr.success('Password successfully reset');
        this.router.navigate(['/login']);
      },
      error: () => {
        this.toastr.error('Failed to reset password');
        this.router.navigate(['/login']);
      },
    });
  }
}
