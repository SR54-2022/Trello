import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from "@angular/forms";
import {animate, state, style, transition, trigger} from "@angular/animations";
import {ToastrService} from "ngx-toastr";
import {AccountRequest} from "../models/account-request.model";
import {AccountService} from "../services/account.service";
import {passwordValidator} from "../validator/password-validator";

@Component({
  selector: 'app-registration',
  templateUrl: './registration.component.html',
  styleUrl: './registration.component.css',
  animations: [
    trigger('flyInOut', [
      state('in', style({ opacity: 1 })),
      transition('void => *', [
        style({ opacity: 0 }),
        animate(300)
      ]),
      transition('* => void', [
        animate(300, style({ opacity: 0 }))
      ])
    ])
  ]
})
export class RegistrationComponent implements OnInit{
  registrationForm!: FormGroup;
  private isSubmitting: boolean = false;

  constructor(
    private readonly formBuilder: FormBuilder,
    private readonly accountService: AccountService,
    private readonly toaster: ToastrService,
  ){}

  ngOnInit(): void {
    this.registrationForm = this.formBuilder.group({
      email: ['',
        [Validators.required, Validators.email]
      ],
      password: ['',
        [Validators.required, passwordValidator()]
      ],
      firstName: ['', [Validators.required]],
      lastName: ['', [Validators.required]],
      role: ['', [Validators.required]],
    });
  }

  onSubmit() {
    if (this.registrationForm.valid && !this.isSubmitting) {
      this.isSubmitting = true; // Set loading state
      const accountRequest: AccountRequest = {
        email: this.registrationForm.get('email')?.value,
        password: this.registrationForm.get('password')?.value,
        first_name: this.registrationForm.get('firstName')?.value,
        last_name: this.registrationForm.get('lastName')?.value,
        role: this.registrationForm.get('role')?.value,

      };
      this.accountService.register(accountRequest).subscribe({
        next: (result) => {
          if (result && result.message) {
            this.toaster.success(result.message);
          } else {
            this.toaster.error('Unexpected response format!');
          }
          this.registrationForm.reset();
          this.isSubmitting = false; // Reset loading state
        },
        error: (error) => {
          console.error('Registration error:', error);
          this.toaster.error('Registration is not successful!');
          this.isSubmitting = false; // Reset loading state
          console.log(accountRequest)
        }
      });
    } else {
      this.toaster.error('Form is not valid!');
    }
  }

}
