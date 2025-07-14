import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators, FormControl } from '@angular/forms';
import { LoginRequest } from '../models/login-request';
import { AccountService } from '../services/account.service';
import { ToastrService } from 'ngx-toastr';
import { Router } from '@angular/router';
import { environment } from '../../environments/environment';


@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css'],
})
export class LoginComponent implements OnInit {
  loginForm!: FormGroup;
  showPassword: boolean = false;
  isSubmitting: boolean = false;
  siteKey: string = environment.recaptcha.siteKey;
  captchaResolvedTime: Date | null = null;
  captchaResetTimeout: any;

  constructor(
    private readonly formBuilder: FormBuilder,
    private readonly accountService: AccountService,
    private readonly toastr: ToastrService,
    private readonly router: Router
  ) {}

  ngOnInit(): void {
    this.loginForm = this.formBuilder.group({
      email: ['', [Validators.required, Validators.email]],
      password: ['', [Validators.required]],
      recaptcha: new FormControl('', Validators.required),
    });
  }

  get recaptchaControl() {
    return this.loginForm.get('recaptcha') as FormControl;
  }

  onCaptchaResolved(captchaResponse: string | null) {
    this.recaptchaControl.setValue(captchaResponse);
    this.captchaResolvedTime = new Date();


    if (this.captchaResetTimeout) {
      clearTimeout(this.captchaResetTimeout);
    }
    this.captchaResetTimeout = setTimeout(() => {
      this.resetCaptcha();
    }, 5 * 60 * 1000);
  }

  resetCaptcha() {
    this.recaptchaControl.setValue(null);
    this.captchaResolvedTime = null;
    this.toastr.warning('Please complete the CAPTCHA again.');
  }

  onSubmit() {
    const fiveMinutes = 5 * 60 * 1000;

    if (
      this.loginForm.valid &&
      !this.isSubmitting &&
      this.captchaResolvedTime &&
      new Date().getTime() - this.captchaResolvedTime.getTime() < fiveMinutes
    ) {
      this.isSubmitting = true;

      const accountRequest: LoginRequest = {
        email: this.loginForm.get('email')?.value,
        password: this.loginForm.get('password')?.value,
        recaptchaToken: this.recaptchaControl.value,
      };

      this.accountService.login(accountRequest).subscribe({
        next: (result) => {
          const userId = result.id;
          this.accountService.startTokenVerification(userId);
          localStorage.setItem("role", result.role);
          this.router.navigate(['/projects']);
        },
        error: (error) => {
          console.error('Login error:', error);
          localStorage.removeItem("role");
          this.isSubmitting = false;
          if (error.status === 403) {
            this.toastr.error('You are already logged in');
          } else if (error.status === 500) {
            this.toastr.error('Incorrect credentials.');
          } else {
            this.toastr.error(error.message ?? 'An error occurred during login');
          }
        },
      });
    } else if (!this.captchaResolvedTime || new Date().getTime() - this.captchaResolvedTime.getTime() >= fiveMinutes) {
      this.resetCaptcha();
    } else {
      console.log('Form is not valid!');
    }
  }

  toggleShowPassword(): void {
    this.showPassword = !this.showPassword;
  }
}
