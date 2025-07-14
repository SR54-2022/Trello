import { Injectable } from '@angular/core';
import {BehaviorSubject} from "rxjs";

@Injectable({
  providedIn: 'root'
})
export class FormToggleService {


  private readonly showFormSubject = new BehaviorSubject<boolean>(false);
  showForm$ = this.showFormSubject.asObservable();

  toggleForm(): void {
    const currentState = this.showFormSubject.value;
    this.showFormSubject.next(!currentState);
  }

  setShowForm(state: boolean): void {
    this.showFormSubject.next(state);
  }

  constructor() { }
}
