import { AbstractControl, ValidationErrors, ValidatorFn } from '@angular/forms';

export function passwordValidator(): ValidatorFn {
  const passwordRegex = {
    length: /.{8,}/,
    uppercase: /[A-Z]/,
    lowercase: /[a-z]/,
    number: /\d/,
    specialChar: /[!@#$%^&*]/,
  };

  return (control: AbstractControl): ValidationErrors | null => {
    const value = control.value;

    if (!value) {
      return null;
    }

    const errors: any = {};
    if (!passwordRegex.length.test(value)) errors.length = 'Password must be at least 8 characters long.';
    if (!passwordRegex.uppercase.test(value)) errors.uppercase = 'Password must contain at least one uppercase letter.';
    if (!passwordRegex.lowercase.test(value)) errors.lowercase = 'Password must contain at least one lowercase letter.';
    if (!passwordRegex.number.test(value)) errors.number = 'Password must contain at least one number.';
    if (!passwordRegex.specialChar.test(value)) errors.specialChar = 'Password must contain at least one special character.';

    return Object.keys(errors).length ? errors : null;
  };
}
