import {CanActivateFn, Router} from '@angular/router';
import {AccountService} from "../services/account.service";
import {inject} from "@angular/core";

export const roleGuard: CanActivateFn = (route, state) => {
  const accountService = inject(AccountService);
  const router = inject(Router);

  const role = localStorage.getItem("role");
  const expectedRoles: string = route.data['expectedRoles'];

  console.log("roleee " + role);
  console.log("userId je " + accountService.getUserId());
  console.log("expectedRoles " + expectedRoles);
  const roles: string[] = expectedRoles.split("|");

  if (roles.indexOf(role ?? "") === -1) {
    router.navigate(["/forbidden"]);
    return false;
  }

  return true;
};
