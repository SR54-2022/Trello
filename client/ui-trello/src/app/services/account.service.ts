import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { ConfigService } from './config.service';
import { UserResponse } from '../member-addition/member-addition.component';
import {BehaviorSubject, interval, Observable, Subscription, switchMap} from 'rxjs';
import { AccountRequest } from '../models/account-request.model';
import { LoginRequest } from '../models/login-request';
import {NavigationEnd, Router} from "@angular/router";
import { jwtDecode } from 'jwt-decode';



@Injectable({
  providedIn: 'root'
})
export class AccountService {
  private readonly userIdSource = new BehaviorSubject<string | null>(null);
  private readonly roleSource = new BehaviorSubject<string | null>(null);
  private tokenVerificationSub!: Subscription;

  constructor(private readonly http: HttpClient, private readonly config: ConfigService, private readonly router: Router) {
  }

  initializeTokenVerification() {
    this.router.events.subscribe(event => {
      if (event instanceof NavigationEnd) {
        const publicPrefixes = ['/verify/account', '/register', '/recovery', '/password/recovery', '/magic'];
        const currentUrl = event.urlAfterRedirects;
        console.log("Current URL after navigation:", currentUrl);

        const isPublicRoute = publicPrefixes.some(prefix => currentUrl.startsWith(prefix));
        if (isPublicRoute) {
          console.log('Skipping token verification for public route:', currentUrl);
          return;
        }

        const role = localStorage.getItem("role");
        if (role) {
          this.getUserIdFromBackend().subscribe({
            next: (userId: string) => {
              console.log("User  ID from server:", userId);
              this.userIdSource.next(userId);
              this.startTokenVerification(userId);
            },
            error: (error) => {
              console.error("Error fetching userId:", error);
              this.logout().subscribe({
                complete: () => {
                  this.router.navigate(['/login']);
                }
              });
            }
          });
        } else {
          this.router.navigate(['/login']);
        }
      }
    });
  }

  getUserIdFromBackend(): Observable<string> {
    return this.http.get(this.config.getUserIdFromTokenUrl(), { responseType: 'text' });
  }


  // Getter for idOfUser
  setUserId(userId: string) {
    this.userIdSource.next(userId);
  }

  // Get the userId directly
  getUserId(): string | null {
    if (this.userIdSource.value === null) {
      this.userIdSource.next(this.getUserIdFromToken());
      return this.getUserIdFromToken();
    }
    return this.userIdSource.getValue();
  }

  getTokenFromCookie(): string | null {
    const cookiePattern = /(^| )auth_token=([^;]+)/;
    const matches = cookiePattern.exec(document.cookie);
    return matches ? matches[2] : null;
  }

  register(accountRequest: AccountRequest): Observable<any> {
    return this.http.post(this.config.register_url, accountRequest);
  }

  getAllUsers(): Observable<UserResponse[]> {
    return this.http.get<UserResponse[]>(this.config.users_url);
  }

  changePassword(newPassword: string): Observable<any> {
    const payload = {
      password: newPassword
    };
    return this.http.post(this.config.change_password_url, payload);
  }


  login(loginCredentials: LoginRequest): Observable<any> {
    return this.http.post<any>(this.config.login_url, loginCredentials);
  }

  logout(): Observable<any> {
    this.stopTokenVerification();
    localStorage.removeItem("role");
    return this.http.post(this.config.logout_url, null);
  }



  startTokenVerification(userId: string) {
    console.log("Token verification started for user:", userId);

    this.setUserId(userId);
    const key = this.getUserId();

    if (this.tokenVerificationSub) {
      this.tokenVerificationSub.unsubscribe();
    }

    this.tokenVerificationSub = interval(60000) // set it to 1 minute
      .pipe(
        switchMap(() => {
          console.log(`[Verification] Checking token for user: ${key} at ${new Date().toLocaleTimeString()}`);
          const headers = new HttpHeaders().set('X-User -ID', key!);
          return this.http.get<boolean>(this.config.verify_token_url, {headers});
        })
      )
      .subscribe({
        next: (isTokenValid) => {
          console.log(`[Response] Token valid: ${isTokenValid} for user: ${key}`);
          if (!isTokenValid) {
            console.warn("[Warning] Token expired. Logging out...");
            this.logout().subscribe({
              complete: () => {
                this.router.navigate(['/login']);
              }
            });
          }
        },
        error: (error) => {
          console.error("[Error] Error verifying token:", error);
          if (localStorage.getItem("role")) {
            localStorage.removeItem("role");
          }
          this.stopTokenVerification();
          this.router.navigate(['/login']);
        },
        complete: () => {
          console.log(`[Complete] Token verification stopped for user: ${key}`);
        }
      });
  }



  stopTokenVerification() {
    if (this.tokenVerificationSub) {
      this.tokenVerificationSub.unsubscribe();
      if (localStorage.getItem("role")) {
        localStorage.removeItem("role");
      }
    }
  }

  checkPassword(password : string): Observable<boolean> {
    const key = this.getUserId();
    const payload = {
      id: key,
      password: password
    };
    return this.http.post<boolean>(this.config.password_check_url, payload);
  }

  requestPasswordReset(email: string): Observable<any> {
    return this.http.post(this.config.recovery_password_url, { email })
  }

  resetPassword(email: string, newPassword: string) {
    const payload = {
      email: email,
      password: newPassword
    }

    return this.http.post(this.config.reset_password_url, payload)
  }

  sendMagicLink(email: string) {
    return this.http.post(this.config.magic_link_url, {email})
  }

  verifyMagic(email: string): Observable<string> {
    return this.http.post<string>(this.config.verify_magic_url, {email})
  }

  getUserIdFromToken(): string | null {
    const token = this.getTokenFromCookie();
    console.log(token);
    if (token) {
      try {
        const decodedToken = jwtDecode<{ user_id: string }>(token);
        console.log(decodedToken);
        return decodedToken.user_id;
      } catch (error) {
        console.error('Error decoding JWT:', error);
        return null;
      }
    }
    return null;
  }

  getRole(email: string): Observable<string> {
    return this.http.post<string>(this.config.get_role_url, { email });
  }

  verifyAccount(email: string): Observable<any> {
    return this.http.get(this.config.verify_account_url(email))
  }


}
