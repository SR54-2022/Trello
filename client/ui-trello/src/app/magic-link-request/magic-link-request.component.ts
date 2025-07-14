import { Component } from '@angular/core';
import {AccountService} from "../services/account.service";
import {ToastrService} from "ngx-toastr";

@Component({
  selector: 'app-magic-link-request',
  templateUrl: './magic-link-request.component.html',
  styleUrl: './magic-link-request.component.css'
})
export class MagicLinkRequestComponent {
  email!: string;

  constructor(private service: AccountService, private toastr: ToastrService) {
  }

  requestMagicLink() {
    this.service.sendMagicLink(this.email).subscribe({
      next: () => {
        this.toastr.success('Magic link sent. Please check your inbox.');
        this.email = ''
      },
      error: (err) => {
        this.toastr.error(err.error?.message ?? err.error ?? 'An unexpected error occurred');
    }
    })

  }
}
