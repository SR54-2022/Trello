import { Component } from '@angular/core';
import {AccountService} from "../services/account.service";
import {ToastrService} from "ngx-toastr";

@Component({
  selector: 'app-password-recovery-request',
  templateUrl: './password-recovery-request.component.html',
  styleUrl: './password-recovery-request.component.css'
})
export class PasswordRecoveryRequestComponent {
  email: string = '';

  constructor(private readonly accountService: AccountService, private readonly toastr: ToastrService) {}

  requestPasswordReset() {
    this.accountService.requestPasswordReset(this.email).subscribe({
      next: () => {
        this.email = '';
        this.toastr.success('Recovery email sent. Please check your inbox.');
      },
      error: (err) => {
        console.log(err);

        const errorMessage = err.error?.message ?? err.error ?? 'An unexpected error occurred';
        this.toastr.error(errorMessage);
      }
    });
  }

}
