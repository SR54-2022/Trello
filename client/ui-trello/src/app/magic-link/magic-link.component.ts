import {Component, OnInit} from '@angular/core';
import {AccountService} from "../services/account.service";
import {ToastrService} from "ngx-toastr";
import {ActivatedRoute, Router} from "@angular/router";

@Component({
  selector: 'app-magic-link',
  templateUrl: './magic-link.component.html',
  styleUrl: './magic-link.component.css'
})
export class MagicLinkComponent implements OnInit {
  loading: boolean = true;
  linkValid: boolean = false;

  constructor(private readonly service: AccountService, private readonly toastr: ToastrService, private readonly router: Router, private readonly route: ActivatedRoute) {
  }

  resendLink() {
    this.router.navigate(["/magic"]);
  }

  goToLogin() {
    this.router.navigate(["/login"]);
  }

  ngOnInit(): void {
    this.verifyMagic();
  }

  verifyMagic() {
    const email = this.route.snapshot.paramMap.get('email') ?? '';
    let role: string;
    this.service.getRole(email).subscribe({
      next: (success) => {
        role = success;
      },
      error: (err) => {
        console.log(err);
      }
    })

    this.service.verifyMagic(email).subscribe({
      next: (response) => {

        setTimeout(() => {
          this.loading = false;

          if (response) {
            localStorage.setItem("role", role);
            this.service.startTokenVerification(response);
            console.log("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")
            console.log(response)
            console.log("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")
            this.linkValid = true;


            setTimeout(() => {
              this.router.navigate(['/projects']);
            }, 10000);
          } else {
            this.linkValid = false;
          }
        }, 5000);
      },
      error: () => {
        setTimeout(() => {
          this.loading = false;
          this.linkValid = false;
        }, 5000);
      }
    });
  }

}
