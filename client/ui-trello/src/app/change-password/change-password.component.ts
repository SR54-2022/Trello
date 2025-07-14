import { Component } from '@angular/core';
import {ToastrService} from "ngx-toastr";
import {AccountService} from "../services/account.service";

@Component({
  selector: 'app-change-password',
  templateUrl: './change-password.component.html',
  styleUrl: './change-password.component.css'
})
export class ChangePasswordComponent {
  oldPassword!: string;
  newPassword!: string;
  repeatNewPassword!: string;
  showPassword: boolean = false;

  constructor(private readonly toastr: ToastrService, private readonly accountService: AccountService) {
  }

  passwordValidation = {
    length: false,
    uppercase: false,
    lowercase: false,
    number: false,
    specialChar: false,
  };

  private readonly passwordRegex = {
    length: /.{8,}/,
    uppercase: /[A-Z]/,
    lowercase: /[a-z]/,
    number: /\d/,
    specialChar: /[!@#$%^&*]/,
  };

  validatePassword() {
    this.passwordValidation.length = this.passwordRegex.length.test(this.newPassword);
    this.passwordValidation.uppercase = this.passwordRegex.uppercase.test(this.newPassword);
    this.passwordValidation.lowercase = this.passwordRegex.lowercase.test(this.newPassword);
    this.passwordValidation.number = this.passwordRegex.number.test(this.newPassword);
    this.passwordValidation.specialChar = this.passwordRegex.specialChar.test(this.newPassword);
  }

  isPasswordValid(): boolean {
    return (
      this.passwordValidation.length &&
      this.passwordValidation.uppercase &&
      this.passwordValidation.lowercase &&
      this.passwordValidation.number &&
      this.passwordValidation.specialChar &&
      this.newPassword === this.repeatNewPassword
    );
  }

  changePassword() {
    if (this.newPassword !== this.repeatNewPassword) {
      this.toastr.error('New passwords do not match.');
      return;
    }


    this.accountService.checkPassword(this.oldPassword).subscribe({
      next: (value) => {
        console.log(value);
        if (!value) {
          this.toastr.error("Incorrect old password.");
          return;
        }
        else if (this.oldPassword === this.newPassword) {
          this.toastr.error("Old and new passwords can't be the same.");
          return;
        }

        this.accountService.changePassword(this.newPassword).subscribe({
          next: (response) => {
            this.toastr.success("You have successfully changed the password");
            this.oldPassword = '';
            this.newPassword = '';
            this.repeatNewPassword = '';
          },
          error: (error) => {
            console.log(error);
            this.toastr.error(error.error);
          },
        });
      },
      error: (err) => {
        this.toastr.error('There has been an error, try again.');
      }
    });
  }



  toggleShowPassword(): void {
    this.showPassword = !this.showPassword;
  }

}
