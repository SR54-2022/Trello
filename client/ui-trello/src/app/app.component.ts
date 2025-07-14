import {Component, OnDestroy, OnInit} from '@angular/core';
import {AccountService} from "./services/account.service";

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnDestroy, OnInit {
  title = 'ui-trello';

  constructor(private readonly accountService: AccountService) {}

  ngOnInit() {
    console.log('AppComponent ngOnInit called');
    this.accountService.initializeTokenVerification();
  }

  ngOnDestroy(): void {
    if (localStorage.getItem("role")) {
      localStorage.removeItem("role");
    }

  }
}
